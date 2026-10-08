//! Raft's durable state (D58): one log file in the WAL framing (D23, D26),
//! holding the [`Record`]s a node asked to persist, in order.
//!
//! ```text
//! file    = header record*                       header magic "SPOOLRFT", first LSN 1
//! body    = 1 term:u64 has_vote:u8 vote:u32      HardState
//!         | 2 index:u64 count:u32 entry*         Append
//!         | 3 from:u64                           Truncate
//! entry   = term:u64 len:u32 data
//! ```
//!
//! Recovery replays every record into a [`Saved`]. A torn tail (D26) is cut
//! off: its records were never synced, so the node never acted on them (it
//! sends nothing before its records are durable). The file grows for ever;
//! compacting it with snapshots is Tier 3.

use super::{Entry, Record, Saved};
use crate::codec::{DecodeError, Reader};
use crate::error::StoreError;
use crate::storage::Storage;
use crate::wal::{self, Format};

pub const FILE: &str = "raft-log";

const FORMAT: Format = Format {
    magic: b"SPOOLRFT",
    version: 1,
    what: "spool raft log",
};

const HARD_STATE: u8 = 1;
const APPEND: u8 = 2;
const TRUNCATE: u8 = 3;

/// What opening found, for coverage counters.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Opened {
    /// The file did not exist (or its creation was cut short) and was made.
    pub created: bool,
    pub records: u64,
    /// A torn tail was cut off.
    pub torn: bool,
}

/// The log file of one node, open for appending.
pub struct RaftLog<S: Storage> {
    storage: S,
    next_lsn: u64,
    poisoned: bool,
}

impl<S: Storage> RaftLog<S> {
    /// Open the log in `storage`, creating it if there is none, and return
    /// what the node saved before.
    pub fn open(mut storage: S) -> Result<(RaftLog<S>, Saved, Opened), StoreError> {
        let mut opened = Opened::default();
        let mut saved = Saved::default();
        let mut next_lsn = 1;
        let exists = storage.list()?.iter().any(|n| n == FILE);
        let scan = if exists {
            let bytes = storage.read(FILE)?;
            let scan = wal::scan_as(&FORMAT, FILE, &bytes, 1, decode)?;
            if scan.header && scan.torn(bytes.len()) {
                storage.truncate(FILE, scan.valid_len)?;
                storage.sync(FILE)?;
                opened.torn = true;
            }
            Some(scan)
        } else {
            None
        };
        match scan {
            Some(scan) if scan.header => {
                for (lsn, record) in scan.records {
                    saved
                        .replay(record)
                        .map_err(|what| StoreError::Corruption {
                            file: FILE.into(),
                            offset: 0,
                            what: format!("record {lsn}: {what}"),
                        })?;
                    opened.records += 1;
                    next_lsn = lsn + 1;
                }
            }
            // Never written past a header that did not make it: start over.
            other => {
                if other.is_some() {
                    storage.remove(FILE)?;
                }
                storage.create(FILE)?;
                let mut header = Vec::new();
                wal::encode_header_as(&FORMAT, 1, &mut header);
                storage.append(FILE, &header)?;
                storage.sync(FILE)?;
                storage.sync_dir()?;
                opened.created = true;
            }
        }
        let log = RaftLog {
            storage,
            next_lsn,
            poisoned: false,
        };
        Ok((log, saved, opened))
    }

    /// Append `records` and sync: when this returns `Ok` they are durable.
    /// After an error the log is poisoned, since some of them may be on disk;
    /// the node must stop and recover (D63).
    pub fn write(&mut self, records: &[Record]) -> Result<(), StoreError> {
        if self.poisoned {
            return Err(StoreError::Poisoned);
        }
        if records.is_empty() {
            return Ok(());
        }
        let mut bytes = Vec::new();
        for record in records {
            wal::encode_frame(self.next_lsn, &mut bytes, |out| encode(record, out));
            self.next_lsn += 1;
        }
        let result = self
            .storage
            .append(FILE, &bytes)
            .and_then(|()| self.storage.sync(FILE));
        if result.is_err() {
            self.poisoned = true;
        }
        Ok(result?)
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }
}

fn encode(record: &Record, out: &mut Vec<u8>) {
    match record {
        Record::HardState { term, vote } => {
            out.push(HARD_STATE);
            out.extend_from_slice(&term.to_le_bytes());
            out.push(vote.is_some() as u8);
            out.extend_from_slice(&vote.unwrap_or(0).to_le_bytes());
        }
        Record::Append { index, entries } => {
            out.push(APPEND);
            out.extend_from_slice(&index.to_le_bytes());
            let count = u32::try_from(entries.len()).expect("under 4G entries");
            out.extend_from_slice(&count.to_le_bytes());
            for e in entries {
                out.extend_from_slice(&e.term.to_le_bytes());
                let len = u32::try_from(e.data.len()).expect("entry under 4 GiB");
                out.extend_from_slice(&len.to_le_bytes());
                out.extend_from_slice(&e.data);
            }
        }
        Record::Truncate { from } => {
            out.push(TRUNCATE);
            out.extend_from_slice(&from.to_le_bytes());
        }
    }
}

fn decode(bytes: &[u8]) -> Result<Record, DecodeError> {
    let mut r = Reader::new(bytes);
    let record = match r.u8()? {
        HARD_STATE => {
            let term = r.u64()?;
            let has_vote = r.u8()?;
            let vote = r.u32()?;
            let vote = match has_vote {
                0 => None,
                1 => Some(vote),
                _ => return Err(DecodeError::Invalid(format!("vote flag {has_vote}"))),
            };
            Record::HardState { term, vote }
        }
        APPEND => {
            let index = r.u64()?;
            let count = r.u32()?;
            let mut entries = Vec::new();
            for _ in 0..count {
                let term = r.u64()?;
                let len = r.u32()?;
                let data = r.bytes(len as usize)?.to_vec();
                entries.push(Entry { term, data });
            }
            Record::Append { index, entries }
        }
        TRUNCATE => Record::Truncate { from: r.u64()? },
        tag => {
            return Err(DecodeError::UnknownTag {
                what: "raft record",
                tag,
            });
        }
    };
    r.finish()?;
    Ok(record)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::storage::MemStorage;

    fn entry(term: u64, data: &str) -> Entry {
        Entry {
            term,
            data: data.as_bytes().to_vec(),
        }
    }

    /// A history that uses every record type: two elections, entries, a
    /// truncation and a replacement.
    fn records() -> Vec<Vec<Record>> {
        vec![
            vec![Record::HardState {
                term: 1,
                vote: Some(2),
            }],
            vec![Record::Append {
                index: 1,
                entries: vec![entry(1, ""), entry(1, "a")],
            }],
            vec![Record::Append {
                index: 3,
                entries: vec![entry(1, "b")],
            }],
            vec![Record::HardState {
                term: 2,
                vote: None,
            }],
            vec![
                Record::Truncate { from: 3 },
                Record::Append {
                    index: 3,
                    entries: vec![entry(2, ""), entry(2, "c")],
                },
            ],
        ]
    }

    /// The `Saved` after each batch of `records()`.
    fn expected() -> Vec<Saved> {
        let mut saved = Saved::default();
        let mut out = vec![saved.clone()];
        for batch in records() {
            for r in batch {
                saved.replay(r).unwrap();
            }
            out.push(saved.clone());
        }
        out
    }

    fn write_all(storage: &mut MemStorage) -> Vec<usize> {
        let (mut log, saved, opened) = RaftLog::open(&mut *storage).unwrap();
        assert!(opened.created);
        assert_eq!(saved, Saved::default());
        let mut ends = vec![log.storage().read(FILE).unwrap().len()];
        for batch in records() {
            log.write(&batch).unwrap();
            ends.push(log.storage().read(FILE).unwrap().len());
        }
        ends
    }

    #[test]
    fn reopening_gives_back_what_was_written() {
        let mut disk = MemStorage::new();
        write_all(&mut disk);
        let (_, saved, opened) = RaftLog::open(&mut disk).unwrap();
        assert_eq!(&saved, expected().last().unwrap());
        assert_eq!(opened.records, 6);
        assert!(!opened.created && !opened.torn);
        assert_eq!(
            saved.log,
            [entry(1, ""), entry(1, "a"), entry(2, ""), entry(2, "c")]
        );
    }

    #[test]
    fn a_cut_at_every_byte_recovers_the_whole_batches_before_it() {
        let mut disk = MemStorage::new();
        let ends = write_all(&mut disk);
        let bytes = disk.read(FILE).unwrap();
        let expected = expected();
        for len in 0..=bytes.len() {
            // Zeros fill only what was never synced, which a header is.
            let zero_tails: &[bool] = if len < wal::HEADER_LEN {
                &[false]
            } else {
                &[false, true]
            };
            for &zeros in zero_tails {
                let mut file = bytes[..len].to_vec();
                if zeros {
                    file.resize(bytes.len(), 0);
                }
                let files = BTreeMap::from([(FILE.to_string(), file.clone())]);
                let mut d = MemStorage::from_files(files);
                let (mut log, saved, opened) = RaftLog::open(&mut d).unwrap();
                // A batch is one append; a cut inside the last batch of two
                // records may keep its first record.
                // (A zero tail can recreate the zero bytes it replaced.)
                let whole = ends
                    .iter()
                    .filter(|&&e| e <= file.len() && file[..e] == bytes[..e])
                    .count();
                let want = if whole == 0 {
                    &expected[0]
                } else {
                    &expected[whole - 1]
                };
                let truncated_only = Saved {
                    term: 2,
                    vote: None,
                    log: vec![entry(1, ""), entry(1, "a")],
                };
                assert!(
                    saved == *want || (whole == 5 && saved == truncated_only),
                    "cut at {len} zeros={zeros}: {saved:?}"
                );
                assert_eq!(opened.created, len < wal::HEADER_LEN);
                // The log is usable afterwards and the cut tail is gone.
                log.write(&[Record::HardState {
                    term: 9,
                    vote: None,
                }])
                .unwrap();
                let (_, again, _) = RaftLog::open(&mut d).unwrap();
                assert_eq!(again.term, 9, "cut at {len}");
                assert_eq!(again.log, saved.log);
            }
        }
    }

    #[test]
    fn every_crash_image_of_a_fresh_log_opens() {
        let mut disk = MemStorage::new();
        RaftLog::open(&mut disk).unwrap();
        for image in disk.crash_images() {
            let (_, saved, _) = RaftLog::open(image).unwrap();
            assert_eq!(saved, Saved::default());
        }
    }

    #[test]
    fn a_failed_write_poisons_the_log() {
        let mut disk = MemStorage::new();
        let (mut log, _, _) = RaftLog::open(&mut disk).unwrap();
        log.storage.fail_in(1); // the append succeeds, the sync fails
        let batch = [Record::HardState {
            term: 1,
            vote: None,
        }];
        assert!(matches!(log.write(&batch), Err(StoreError::Io(_))));
        assert!(matches!(log.write(&batch), Err(StoreError::Poisoned)));
    }

    #[test]
    fn damage_before_the_end_and_records_that_do_not_fit_are_corruption() {
        let mut disk = MemStorage::new();
        let ends = write_all(&mut disk);
        let mut bytes = disk.read(FILE).unwrap();
        bytes[ends[1] + wal::RECORD_HEADER_LEN] ^= 1;
        let d = MemStorage::from_files(BTreeMap::from([(FILE.to_string(), bytes)]));
        assert!(matches!(
            RaftLog::open(d),
            Err(StoreError::Corruption { .. })
        ));

        // Well-framed, but an append that leaves a gap.
        let mut d = MemStorage::new();
        let (mut log, _, _) = RaftLog::open(&mut d).unwrap();
        log.write(&[Record::Append {
            index: 2,
            entries: vec![entry(1, "")],
        }])
        .unwrap();
        match RaftLog::open(&mut d) {
            Err(StoreError::Corruption { what, .. }) => {
                assert!(what.contains("append at 2"), "{what}")
            }
            other => panic!("{:?}", other.map(|(_, s, _)| s)),
        }
    }

    #[test]
    fn a_queue_segment_is_not_a_raft_log() {
        let mut bytes = Vec::new();
        wal::encode_header(1, &mut bytes);
        let d = MemStorage::from_files(BTreeMap::from([(FILE.to_string(), bytes)]));
        match RaftLog::open(d) {
            Err(StoreError::Corruption { what, .. }) => assert_eq!(what, "not a spool raft log"),
            other => panic!("{:?}", other.map(|(_, s, _)| s)),
        }
    }
}
