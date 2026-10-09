//! The durable queue (M2): a state machine (D4) wrapped in a write-ahead log
//! of commands (D21) and periodic snapshots (D27), recovered on open (D29).
//!
//! Directory layout:
//! - `wal-<first lsn>`: log segments (D23). Only the last one is appended to.
//! - `snap-<lsn>`: the state after every command up to `lsn` (D27).
//! - `*.tmp`: a snapshot being written; deleted on open.
//!
//! Every storage error poisons the handle (D25): the in-memory state may be
//! ahead of the disk, so the only way forward is to reopen.

use std::path::Path;

use crate::command::{Command, Event};
use crate::error::StoreError;
use crate::queue::Snapshot;
use crate::reference::ReferenceQueue;
use crate::storage::{FileStorage, Storage};
use crate::wal::{self, parse_lsn_name, parse_segment_name, segment_name};

const SNAP_PREFIX: &str = "snap-";
const SNAP_MAGIC: &[u8; 8] = b"SPOOLSNP";
/// `magic version:u32 lsn:u64 body_len:u64 crc:u32`, crc over the first 28
/// bytes and the body.
const SNAP_HEADER_LEN: usize = 32;

#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// Take a snapshot after this many commands since the last one (D27).
    /// 0 means only when `snapshot` is called.
    pub snapshot_every: u64,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            snapshot_every: 10_000,
        }
    }
}

/// What `open` found.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Recovery {
    /// LSN of the snapshot loaded, 0 if none.
    pub snapshot_lsn: u64,
    /// Commands replayed from the log after the snapshot.
    pub replayed: u64,
    /// Bytes of torn tail cut off the last segment.
    pub truncated: u64,
}

pub struct Durable<S: Storage, Q: Snapshot = ReferenceQueue> {
    storage: S,
    queue: Q,
    options: Options,
    /// The segment new records are appended to.
    segment: String,
    /// LSN the next command gets. LSNs start at 1.
    next_lsn: u64,
    /// Commands logged since the last snapshot.
    since_snapshot: u64,
    poisoned: bool,
    /// Reused record buffer and event buffer (D11).
    buf: Vec<u8>,
    out: Vec<Event>,
}

impl<Q: Snapshot> Durable<FileStorage, Q> {
    /// Open (or create) the data directory `dir`, locking it (D29).
    pub fn open_dir(dir: &Path, options: Options) -> Result<(Self, Recovery), StoreError> {
        Self::open(FileStorage::open(dir)?, options)
    }
}

impl<S: Storage, Q: Snapshot> Durable<S, Q> {
    /// Recover the queue from `storage` (D29): newest snapshot, then the log
    /// after it, cutting a torn tail off the last segment (D26).
    pub fn open(mut storage: S, options: Options) -> Result<(Self, Recovery), StoreError> {
        let names = storage.list()?;
        let mut cleaned = false;
        for name in names.iter().filter(|n| n.ends_with(".tmp")) {
            storage.remove(name)?;
            cleaned = true;
        }
        // `list` is sorted and the LSNs are zero-padded, so these are in order.
        let snaps: Vec<u64> = names
            .iter()
            .filter_map(|n| parse_lsn_name(n, SNAP_PREFIX))
            .collect();
        let segments: Vec<u64> = names.iter().filter_map(|n| parse_segment_name(n)).collect();

        let mut recovery = Recovery::default();
        let mut queue = match snaps.last() {
            Some(&lsn) => {
                recovery.snapshot_lsn = lsn;
                load_snapshot(&storage, lsn)?
            }
            None => Q::default(),
        };
        let first_needed = recovery.snapshot_lsn + 1;

        // The segment holding `first_needed`; older ones are left over from a
        // crash between a snapshot and its cleanup.
        let start = segments.iter().rposition(|&first| first <= first_needed);
        if start.is_none() && !segments.is_empty() {
            return Err(StoreError::Corruption {
                file: segment_name(segments[0]),
                offset: 0,
                what: format!(
                    "log starts at LSN {}, but the snapshot ends at LSN {}",
                    segments[0], recovery.snapshot_lsn
                ),
            });
        }
        let live = start.map_or(&[][..], |s| &segments[s..]);
        let mut next_lsn = first_needed;
        let mut out = Vec::new();
        for (i, &first) in live.iter().enumerate() {
            let name = segment_name(first);
            if i > 0 && first != next_lsn {
                return Err(StoreError::Corruption {
                    file: name,
                    offset: 0,
                    what: format!("segment starts at LSN {first}, expected {next_lsn}"),
                });
            }
            let bytes = storage.read(&name)?;
            let scan = wal::scan(&name, &bytes, first)?;
            let last = i + 1 == live.len();
            if !last && (!scan.header || scan.torn(bytes.len())) {
                return Err(StoreError::Corruption {
                    file: name,
                    offset: scan.valid_len,
                    what: "torn data in a segment that is not the last".into(),
                });
            }
            next_lsn = first + scan.records.len() as u64;
            for (lsn, cmd) in &scan.records {
                if *lsn > recovery.snapshot_lsn {
                    queue.apply(cmd, &mut out);
                    out.clear();
                    recovery.replayed += 1;
                }
            }
            if last {
                // Cut the torn tail before anything is appended after it (D26).
                if !scan.header {
                    storage.truncate(&name, 0)?;
                    let mut header = Vec::new();
                    wal::encode_header(first, &mut header);
                    storage.append(&name, &header)?;
                    storage.sync(&name)?;
                    recovery.truncated = bytes.len() as u64;
                } else if scan.torn(bytes.len()) {
                    storage.truncate(&name, scan.valid_len)?;
                    storage.sync(&name)?;
                    recovery.truncated = bytes.len() as u64 - scan.valid_len;
                }
            }
        }
        if next_lsn < first_needed {
            return Err(StoreError::Corruption {
                file: segment_name(*live.last().expect("live segments exist")),
                offset: 0,
                what: format!(
                    "log ends at LSN {}, before the snapshot's LSN {}",
                    next_lsn - 1,
                    recovery.snapshot_lsn
                ),
            });
        }

        let mut durable = Durable {
            storage,
            queue,
            options,
            segment: String::new(),
            next_lsn,
            since_snapshot: next_lsn - first_needed,
            poisoned: false,
            buf: Vec::new(),
            out,
        };
        match live.last() {
            Some(&first) => durable.segment = segment_name(first),
            None => durable.start_segment(first_needed)?,
        }
        // Only now that the snapshot and the live log are in place.
        let obsolete = segments[..start.unwrap_or(0)]
            .iter()
            .map(|&f| segment_name(f))
            .chain(snaps.iter().rev().skip(1).map(|&l| snap_name(l)));
        for name in obsolete {
            durable.storage.remove(&name)?;
            cleaned = true;
        }
        if cleaned {
            durable.storage.sync_dir()?;
        }
        Ok((durable, recovery))
    }

    /// Apply `cmd` and return its events once its record is synced (D22).
    pub fn apply(&mut self, cmd: &Command) -> Result<&[Event], StoreError> {
        self.check()?;
        self.out.clear();
        // Nobody sees the new state until the sync below succeeds; if it fails,
        // the handle is poisoned and the state is thrown away.
        self.queue.apply(cmd, &mut self.out);
        self.buf.clear();
        wal::encode_record(self.next_lsn, cmd, &mut self.buf);
        self.commit(1)?;
        Ok(&self.out)
    }

    /// Apply several commands with one append and one sync (D25): each
    /// command's events, returned only after all of them are durable.
    pub fn apply_batch(&mut self, cmds: &[Command]) -> Result<Vec<Vec<Event>>, StoreError> {
        self.check()?;
        self.buf.clear();
        let mut events = Vec::with_capacity(cmds.len());
        for (lsn, cmd) in (self.next_lsn..).zip(cmds) {
            let mut out = Vec::new();
            self.queue.apply(cmd, &mut out);
            events.push(out);
            wal::encode_record(lsn, cmd, &mut self.buf);
        }
        if !cmds.is_empty() {
            self.commit(cmds.len() as u64)?;
        }
        Ok(events)
    }

    /// Write the records in `buf`, sync, and snapshot if it is time.
    fn commit(&mut self, records: u64) -> Result<(), StoreError> {
        let written = self
            .storage
            .append(&self.segment, &self.buf)
            .and_then(|()| self.storage.sync(&self.segment));
        if let Err(e) = written {
            self.poisoned = true;
            return Err(e.into());
        }
        self.next_lsn += records;
        self.since_snapshot += records;
        if self.options.snapshot_every > 0 && self.since_snapshot >= self.options.snapshot_every {
            // The commands above are durable even if this fails.
            self.snapshot()?;
        }
        Ok(())
    }

    /// Snapshot the state, start a new segment and delete what the snapshot
    /// replaces (D27). Does nothing if no command was logged since the last one.
    pub fn snapshot(&mut self) -> Result<(), StoreError> {
        self.check()?;
        if self.since_snapshot == 0 {
            return Ok(());
        }
        let result = self.write_snapshot();
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }

    fn write_snapshot(&mut self) -> Result<(), StoreError> {
        let lsn = self.next_lsn - 1;
        let name = snap_name(lsn);
        let tmp = format!("{name}.tmp");
        let mut bytes = Vec::new();
        encode_snapshot(&self.queue, lsn, &mut bytes);
        self.storage.create(&tmp)?;
        self.storage.append(&tmp, &bytes)?;
        self.storage.sync(&tmp)?;
        self.storage.rename(&tmp, &name)?;
        // `start_segment` syncs the directory again before anything is deleted,
        // so this one is not needed for safety; it makes the snapshot durable
        // at the step that creates it instead of depending on the next step.
        self.storage.sync_dir()?;
        // From here the snapshot is the recovery point; the old log is garbage.
        self.start_segment(lsn + 1)?;
        for old in self.storage.list()? {
            let replaced = parse_lsn_name(&old, SNAP_PREFIX).is_some_and(|l| l < lsn)
                || parse_segment_name(&old).is_some_and(|f| f <= lsn);
            if replaced {
                self.storage.remove(&old)?;
            }
        }
        self.storage.sync_dir()?;
        self.since_snapshot = 0;
        Ok(())
    }

    /// Create segment `wal-<first>` with its header, durable before any record
    /// goes into it (D26).
    fn start_segment(&mut self, first: u64) -> Result<(), StoreError> {
        let name = segment_name(first);
        let mut header = Vec::new();
        wal::encode_header(first, &mut header);
        self.storage.create(&name)?;
        self.storage.append(&name, &header)?;
        self.storage.sync(&name)?;
        self.storage.sync_dir()?;
        self.segment = name;
        Ok(())
    }

    fn check(&self) -> Result<(), StoreError> {
        if self.poisoned {
            return Err(StoreError::Poisoned);
        }
        Ok(())
    }

    /// The in-memory state. After a storage error it may be ahead of the disk.
    pub fn queue(&self) -> &Q {
        &self.queue
    }

    /// The LSN the next command will get; `next_lsn() - 1` commands are durable.
    pub fn next_lsn(&self) -> u64 {
        self.next_lsn
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

    pub fn into_storage(self) -> S {
        self.storage
    }
}

fn snap_name(lsn: u64) -> String {
    format!("{SNAP_PREFIX}{lsn:020}")
}

fn encode_snapshot<Q: Snapshot>(queue: &Q, lsn: u64, out: &mut Vec<u8>) {
    out.extend_from_slice(SNAP_MAGIC);
    out.extend_from_slice(&Q::STATE_VERSION.to_le_bytes());
    out.extend_from_slice(&lsn.to_le_bytes());
    out.extend_from_slice(&[0; 12]); // body length and CRC, filled in below
    queue.encode_state(out);
    let body_len = (out.len() - SNAP_HEADER_LEN) as u64;
    out[20..28].copy_from_slice(&body_len.to_le_bytes());
    let crc = crc32c::crc32c_append(crc32c::crc32c(&out[..28]), &out[SNAP_HEADER_LEN..]);
    out[28..32].copy_from_slice(&crc.to_le_bytes());
}

/// Load `snap-<lsn>`. Snapshots are renamed into place only after a sync, so
/// any damage here is corruption, never a crash (D27).
fn load_snapshot<S: Storage, Q: Snapshot>(storage: &S, lsn: u64) -> Result<Q, StoreError> {
    let name = snap_name(lsn);
    let bytes = storage.read(&name)?;
    let corrupt = |offset: usize, what: String| StoreError::Corruption {
        file: name.clone(),
        offset: offset as u64,
        what,
    };
    if bytes.len() < SNAP_HEADER_LEN || &bytes[..8] != SNAP_MAGIC {
        return Err(corrupt(0, "not a spool snapshot".into()));
    }
    let field = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    let crc = u32::from_le_bytes(bytes[28..32].try_into().unwrap());
    let body = &bytes[SNAP_HEADER_LEN..];
    if crc32c::crc32c_append(crc32c::crc32c(&bytes[..28]), body) != crc {
        return Err(corrupt(28, "snapshot checksum mismatch".into()));
    }
    if version == 0 || version > Q::STATE_VERSION {
        return Err(corrupt(8, format!("unknown snapshot version {version}")));
    }
    if field(12) != lsn || field(20) != body.len() as u64 {
        return Err(corrupt(12, "header disagrees with the file".into()));
    }
    Q::decode_state(version, body).map_err(|e| corrupt(SNAP_HEADER_LEN, e.to_string()))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::queue::Queue;
    use crate::storage::MemStorage;

    type Mem = Durable<MemStorage>;

    const CMDS: [&str; 8] = [
        "@0 enqueue q a",
        "@0 enqueue q b",
        "@1 lease q 10",
        "@2 ack 1 1",
        "@3 configure q 1 0 0",
        "@4 lease q 10",
        "@5 nack 2 2",
        "@6 enqueue r c delay=5",
    ];

    fn cmd(i: usize) -> Command {
        CMDS[i].parse().unwrap()
    }

    /// The reference state after the first `n` commands, encoded.
    fn reference(n: usize) -> Vec<u8> {
        let mut q = ReferenceQueue::new();
        let mut out = Vec::new();
        (0..n).for_each(|i| q.apply(&cmd(i), &mut out));
        state(&q)
    }

    fn state(q: &ReferenceQueue) -> Vec<u8> {
        let mut out = Vec::new();
        q.encode_state(&mut out);
        out
    }

    fn opts(every: u64) -> Options {
        Options {
            snapshot_every: every,
        }
    }

    fn run(n: usize, every: u64) -> MemStorage {
        let (mut d, _) = Mem::open(MemStorage::new(), opts(every)).unwrap();
        (0..n).for_each(|i| {
            d.apply(&cmd(i)).unwrap();
        });
        d.into_storage()
    }

    fn reopen(files: BTreeMap<String, Vec<u8>>) -> Result<(Mem, Recovery), StoreError> {
        Mem::open(MemStorage::from_files(files), opts(0))
    }

    fn corruption(r: Result<(Mem, Recovery), StoreError>) -> String {
        match r {
            Err(StoreError::Corruption { file, what, .. }) => format!("{file}: {what}"),
            Err(e) => panic!("expected corruption, got {e}"),
            Ok(_) => panic!("expected corruption, opened fine"),
        }
    }

    #[test]
    fn reopen_replays_the_log() {
        let (d, rec) = reopen(run(8, 0).files()).unwrap();
        assert_eq!(state(d.queue()), reference(8));
        assert_eq!(
            rec,
            Recovery {
                snapshot_lsn: 0,
                replayed: 8,
                truncated: 0
            }
        );
        assert_eq!(d.next_lsn(), 9);
    }

    #[test]
    fn snapshots_replace_the_log() {
        let files = run(8, 3).files();
        let names: Vec<&str> = files.keys().map(String::as_str).collect();
        assert_eq!(
            names,
            ["snap-00000000000000000006", "wal-00000000000000000007"]
        );
        let (mut d, rec) = reopen(files).unwrap();
        assert_eq!((rec.snapshot_lsn, rec.replayed), (6, 2));
        assert_eq!(state(d.queue()), reference(8));
        // A manual snapshot, then one with nothing new, which does nothing.
        d.snapshot().unwrap();
        let files = d.storage().files();
        d.snapshot().unwrap();
        assert_eq!(d.storage().files(), files);
        assert_eq!(files.keys().next().unwrap(), "snap-00000000000000000008");
        let (d, rec) = reopen(files).unwrap();
        assert_eq!((rec.snapshot_lsn, rec.replayed), (8, 0));
        assert_eq!(state(d.queue()), reference(8));
    }

    #[test]
    fn batches_share_one_sync() {
        let (mut d, _) = Mem::open(MemStorage::new(), opts(0)).unwrap();
        let before = d.storage().calls();
        let events = d.apply_batch(&(0..4).map(cmd).collect::<Vec<_>>()).unwrap();
        assert_eq!(d.storage().calls() - before, 2, "one append, one sync");
        assert_eq!(events.len(), 4);
        assert_eq!(events[3][0].to_string(), "acked job=1");
        assert_eq!(d.apply_batch(&[]).unwrap(), Vec::<Vec<Event>>::new());
        let (d, _) = reopen(d.into_storage().files()).unwrap();
        assert_eq!(state(d.queue()), reference(4));
    }

    #[test]
    fn torn_tail_is_cut_and_new_records_survive() {
        let mut storage = run(5, 0);
        storage
            .append("wal-00000000000000000001", &[7, 0, 0])
            .unwrap();
        let (mut d, rec) = Mem::open(storage, opts(0)).unwrap();
        assert_eq!((rec.replayed, rec.truncated), (5, 3));
        for i in 5..8 {
            d.apply(&cmd(i)).unwrap();
        }
        let (d, rec) = reopen(d.into_storage().files()).unwrap();
        assert_eq!((rec.replayed, rec.truncated), (8, 0));
        assert_eq!(state(d.queue()), reference(8));
    }

    #[test]
    fn leftover_files_are_cleaned_up() {
        let mut files = run(8, 3).files();
        let old_log = run(2, 0).files();
        files.extend(old_log);
        files.insert("snap-00000000000000000003".into(), Vec::new());
        files.insert("snap-00000000000000000009.tmp".into(), b"half".to_vec());
        let (d, rec) = reopen(files).unwrap();
        assert_eq!(rec.snapshot_lsn, 6);
        assert_eq!(state(d.queue()), reference(8));
        let names: Vec<String> = d.storage().files().into_keys().collect();
        assert_eq!(
            names,
            ["snap-00000000000000000006", "wal-00000000000000000007"]
        );
    }

    #[test]
    fn refuses_damage_a_crash_cannot_cause() {
        let mut storage = run(5, 0);
        // A flipped bit in record 1, with records 2-5 after it.
        let mut files = storage.files();
        files.get_mut("wal-00000000000000000001").unwrap()[wal::HEADER_LEN + 30] ^= 1;
        assert!(corruption(reopen(files)).contains("checksum"));

        // Snapshot lost: the log starts after LSN 1.
        let mut files = run(8, 3).files();
        files.remove("snap-00000000000000000006");
        assert_eq!(
            corruption(reopen(files)),
            "wal-00000000000000000007: log starts at LSN 7, but the snapshot ends at LSN 0"
        );

        // A damaged snapshot.
        let mut files = run(8, 3).files();
        *files
            .get_mut("snap-00000000000000000006")
            .unwrap()
            .last_mut()
            .unwrap() ^= 1;
        assert!(corruption(reopen(files)).contains("snapshot checksum"));

        // A snapshot newer than the end of the log.
        let mut files = run(3, 0).files();
        files.extend(
            run(8, 6)
                .files()
                .into_iter()
                .filter(|(n, _)| n.starts_with("snap")),
        );
        assert_eq!(
            corruption(reopen(files)),
            "wal-00000000000000000001: log ends at LSN 3, before the snapshot's LSN 6"
        );

        // A gap between segments.
        let mut files = run(2, 0).files();
        let mut later = Vec::new();
        wal::encode_header(5, &mut later);
        files.insert(segment_name(5), later);
        assert!(corruption(reopen(files)).contains("segment starts at LSN 5, expected 3"));

        // A torn record in a segment that is not the last.
        storage.append("wal-00000000000000000001", &[1]).unwrap();
        let mut files = storage.files();
        let mut next = Vec::new();
        wal::encode_header(6, &mut next);
        files.insert(segment_name(6), next);
        assert!(corruption(reopen(files)).contains("not the last"));
    }

    #[test]
    fn loads_a_version_1_snapshot() {
        // Rewrite run(8, 3)'s snapshot as M2 wrote it: version 1, without the
        // key and result tables (D35, D45) and M8's (16 + 22 bytes, empty).
        let mut files = run(8, 3).files();
        let snap = files.get_mut("snap-00000000000000000006").unwrap();
        snap.truncate(snap.len() - 16 - 22);
        reseal(snap, 1);
        let (mut d, rec) = reopen(files).unwrap();
        assert_eq!((rec.snapshot_lsn, rec.replayed), (6, 2));
        assert_eq!(state(d.queue()), reference(8));
        // The next snapshot is written in the current version.
        d.snapshot().unwrap();
        let files = d.storage().files();
        assert_eq!(
            files["snap-00000000000000000008"][8..12],
            ReferenceQueue::STATE_VERSION.to_le_bytes()
        );

        // A version from the future, with a valid checksum, is refused.
        let mut files = run(8, 3).files();
        let snap = files.get_mut("snap-00000000000000000006").unwrap();
        reseal(snap, ReferenceQueue::STATE_VERSION + 1);
        assert!(corruption(reopen(files)).contains("unknown snapshot version"));
    }

    /// Set a snapshot file's version and recompute its length and checksum.
    fn reseal(snap: &mut [u8], version: u32) {
        snap[8..12].copy_from_slice(&version.to_le_bytes());
        let body_len = (snap.len() - SNAP_HEADER_LEN) as u64;
        snap[20..28].copy_from_slice(&body_len.to_le_bytes());
        let crc = crc32c::crc32c_append(crc32c::crc32c(&snap[..28]), &snap[SNAP_HEADER_LEN..]);
        snap[28..32].copy_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn a_storage_error_poisons_the_handle() {
        // Opening a fresh directory takes 4 calls; the first append fails.
        let (mut d, _) = Mem::open(MemStorage::new().fail_after(4), opts(0)).unwrap();
        assert!(matches!(d.apply(&cmd(0)), Err(StoreError::Io(_))));
        assert!(matches!(d.apply(&cmd(0)), Err(StoreError::Poisoned)));
        assert!(matches!(d.apply_batch(&[]), Err(StoreError::Poisoned)));
        assert!(matches!(d.snapshot(), Err(StoreError::Poisoned)));
        // The record's bytes reached the file but were never synced.
        let images = d.storage().crash_images();
        let states: Vec<Vec<u8>> = images
            .into_iter()
            .map(|s| state(Mem::open(s, opts(0)).unwrap().0.queue()))
            .collect();
        assert!(states.contains(&reference(0)) && states.contains(&reference(1)));
    }
}
