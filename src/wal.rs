//! Write-ahead log segments (D23) and the rules for reading them back (D26).
//! Pure functions over bytes: the durable queue does the I/O.
//!
//! ```text
//! segment = header record*
//! header  = "SPOOLWAL" version:u32 first_lsn:u64 crc:u32     24 bytes, crc over the first 20
//! record  = len:u32 crc:u32 lsn:u64 body                     crc over len, lsn and body
//! body    = one command (D24), len bytes
//! ```

use crate::codec::{decode_command, encode_command};
use crate::command::Command;
use crate::error::StoreError;

pub const HEADER_LEN: usize = 24;
pub const RECORD_HEADER_LEN: usize = 16;
const MAGIC: &[u8; 8] = b"SPOOLWAL";
const VERSION: u32 = 1;
const PREFIX: &str = "wal-";

/// `wal-<first lsn>`, zero-padded to 20 digits so names sort by LSN (D29).
pub fn segment_name(first_lsn: u64) -> String {
    format!("{PREFIX}{first_lsn:020}")
}

/// The first LSN in a segment's name, if `name` is a segment.
pub fn parse_segment_name(name: &str) -> Option<u64> {
    parse_lsn_name(name, PREFIX)
}

/// `<prefix><20 digits>` to the number, nothing else.
pub(crate) fn parse_lsn_name(name: &str, prefix: &str) -> Option<u64> {
    let digits = name.strip_prefix(prefix)?;
    if digits.len() != 20 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

pub fn encode_header(first_lsn: u64, out: &mut Vec<u8>) {
    let start = out.len();
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&VERSION.to_le_bytes());
    out.extend_from_slice(&first_lsn.to_le_bytes());
    let crc = crc32c::crc32c(&out[start..]);
    out.extend_from_slice(&crc.to_le_bytes());
}

/// Append one framed record holding `cmd` at `lsn`.
pub fn encode_record(lsn: u64, cmd: &Command, out: &mut Vec<u8>) {
    let start = out.len();
    out.extend_from_slice(&[0; RECORD_HEADER_LEN]);
    encode_command(cmd, out);
    let len = u32::try_from(out.len() - start - RECORD_HEADER_LEN).expect("record under 4 GiB");
    out[start..start + 4].copy_from_slice(&len.to_le_bytes());
    out[start + 8..start + 16].copy_from_slice(&lsn.to_le_bytes());
    let crc = record_crc(&out[start..]);
    out[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
}

/// CRC-32C over a whole record except its own CRC field.
fn record_crc(record: &[u8]) -> u32 {
    crc32c::crc32c_append(crc32c::crc32c(&record[..4]), &record[8..])
}

/// What one segment holds.
#[derive(Debug, PartialEq, Eq)]
pub struct Scan {
    /// False if the file is shorter than a header or its header is damaged
    /// with nothing after it: a crash while the segment was being created.
    pub header: bool,
    /// Every valid record, in order, with its LSN.
    pub records: Vec<(u64, Command)>,
    /// Bytes up to the end of the last valid record (or the header).
    pub valid_len: u64,
}

impl Scan {
    /// True if the file has bytes after its valid part: a torn tail.
    pub fn torn(&self, file_len: usize) -> bool {
        self.valid_len < file_len as u64
    }
}

/// Read the segment `name` that should start at `first_lsn` (D26). A bad record
/// followed only by zeros, or a record cut short, ends the scan: that is a torn
/// tail, and the caller decides whether this segment may have one. Anything
/// else wrong is `Corruption`.
pub fn scan(name: &str, bytes: &[u8], first_lsn: u64) -> Result<Scan, StoreError> {
    let corrupt = |offset: usize, what: String| StoreError::Corruption {
        file: name.to_string(),
        offset: offset as u64,
        what,
    };
    let header_ok = bytes.len() >= HEADER_LEN
        && crc32c::crc32c(&bytes[..20]) == u32::from_le_bytes(bytes[20..24].try_into().unwrap());
    if !header_ok {
        // Records are appended only after the header is synced, so a bad header
        // with more data behind it is damage, not a crash.
        if bytes.len() > HEADER_LEN {
            return Err(corrupt(0, "bad segment header".into()));
        }
        return Ok(Scan {
            header: false,
            records: Vec::new(),
            valid_len: 0,
        });
    }
    if &bytes[..8] != MAGIC {
        return Err(corrupt(0, "not a spool log segment".into()));
    }
    let version = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
    if version != VERSION {
        return Err(corrupt(8, format!("unknown log version {version}")));
    }
    let header_lsn = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
    if header_lsn != first_lsn {
        return Err(corrupt(12, format!("header says first LSN {header_lsn}")));
    }

    let mut records = Vec::new();
    let mut at = HEADER_LEN;
    let mut lsn = first_lsn;
    while at < bytes.len() {
        let rest = &bytes[at..];
        if rest.len() < RECORD_HEADER_LEN {
            break; // a record header cut short
        }
        let len = u32::from_le_bytes(rest[..4].try_into().unwrap());
        let end = RECORD_HEADER_LEN + len as usize;
        if end > rest.len() {
            break; // cut short, or a length that points past the end (D23 known limit)
        }
        let record = &rest[..end];
        let crc = u32::from_le_bytes(record[4..8].try_into().unwrap());
        if record_crc(record) != crc {
            if rest[end..].iter().all(|&b| b == 0) {
                break; // the last thing in the file, or followed only by zeros
            }
            return Err(corrupt(at, "record checksum mismatch".into()));
        }
        let got = u64::from_le_bytes(record[8..16].try_into().unwrap());
        if got != lsn {
            return Err(corrupt(at, format!("record LSN {got}, expected {lsn}")));
        }
        let cmd = decode_command(&record[RECORD_HEADER_LEN..])
            .map_err(|e| corrupt(at, format!("record {lsn}: {e}")))?;
        records.push((lsn, cmd));
        lsn += 1;
        at += end;
    }
    Ok(Scan {
        header: true,
        records,
        valid_len: at as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const CMDS: [&str; 3] = ["@1 enqueue q hello", "@2 lease q 30", "@3 ack 1 1"];

    /// A segment starting at LSN 7 holding `CMDS`, and where each record starts.
    fn segment() -> (Vec<u8>, Vec<usize>) {
        let mut out = Vec::new();
        encode_header(7, &mut out);
        let mut starts = Vec::new();
        for (i, line) in CMDS.iter().enumerate() {
            starts.push(out.len());
            encode_record(7 + i as u64, &line.parse().unwrap(), &mut out);
        }
        (out, starts)
    }

    fn lsns(scan: &Scan) -> Vec<u64> {
        scan.records.iter().map(|(l, _)| *l).collect()
    }

    fn corruption(r: Result<Scan, StoreError>) -> (u64, String) {
        match r {
            Err(StoreError::Corruption { offset, what, .. }) => (offset, what),
            other => panic!("expected corruption, got {other:?}"),
        }
    }

    #[test]
    fn names() {
        assert_eq!(segment_name(42), "wal-00000000000000000042");
        assert_eq!(parse_segment_name(&segment_name(u64::MAX)), Some(u64::MAX));
        for bad in [
            "wal-42",
            "wal-0000000000000000004x",
            "snap-00000000000000000042",
        ] {
            assert_eq!(parse_segment_name(bad), None, "{bad}");
        }
        assert!(segment_name(9) < segment_name(10), "names sort by LSN");
    }

    #[test]
    fn reads_back_every_record() {
        let (bytes, _) = segment();
        let s = scan("w", &bytes, 7).unwrap();
        assert!(s.header && !s.torn(bytes.len()));
        assert_eq!(lsns(&s), [7, 8, 9]);
        assert_eq!(s.records[2].1, CMDS[2].parse().unwrap());
    }

    #[test]
    fn every_cut_is_a_torn_tail_after_the_complete_records() {
        let (bytes, starts) = segment();
        // Where each record ends.
        let ends: Vec<usize> = starts[1..].iter().copied().chain([bytes.len()]).collect();
        for len in 0..bytes.len() {
            let s = scan("w", &bytes[..len], 7).unwrap();
            if len < HEADER_LEN {
                assert!(!s.header, "{len}");
                continue;
            }
            let complete = ends.iter().filter(|&&e| e <= len).count();
            assert_eq!(s.records.len(), complete, "cut at {len}");
            let valid = if complete == 0 {
                HEADER_LEN
            } else {
                ends[complete - 1]
            };
            assert_eq!(s.valid_len as usize, valid, "cut at {len}");
            assert_eq!(s.torn(len), len != valid);
        }
    }

    #[test]
    fn zero_tails_are_torn_not_corrupt() {
        let (mut bytes, starts) = segment();
        bytes.truncate(starts[2]);
        bytes.extend_from_slice(&[0; 40]);
        let s = scan("w", &bytes, 7).unwrap();
        assert_eq!(lsns(&s), [7, 8]);
        assert_eq!(s.valid_len as usize, starts[2]);
        // An all-zero header is a segment whose creation was cut short.
        assert!(!scan("w", &[0; HEADER_LEN], 7).unwrap().header);
    }

    #[test]
    fn damage_with_data_after_it_is_corruption() {
        let (bytes, starts) = segment();
        // A flipped bit in the middle record's body.
        let mut b = bytes.clone();
        b[starts[1] + RECORD_HEADER_LEN + 2] ^= 1;
        assert_eq!(
            corruption(scan("w", &b, 7)),
            (starts[1] as u64, "record checksum mismatch".into())
        );
        // The same damage in the last record looks like a torn write.
        let mut b = bytes.clone();
        b[starts[2] + RECORD_HEADER_LEN] ^= 1;
        assert_eq!(lsns(&scan("w", &b, 7).unwrap()), [7, 8]);
        // A flipped length bit is covered by the CRC.
        let mut b = bytes.clone();
        b[starts[0]] ^= 1;
        assert_eq!(corruption(scan("w", &b, 7)).0, starts[0] as u64);
        // A bad header with records behind it.
        let mut b = bytes.clone();
        b[3] ^= 1;
        assert_eq!(corruption(scan("w", &b, 7)).0, 0);
    }

    #[test]
    fn lsns_must_match_the_name_and_be_consecutive() {
        let (bytes, _) = segment();
        assert_eq!(
            corruption(scan("w", &bytes, 6)),
            (12, "header says first LSN 7".into())
        );
        let mut b = Vec::new();
        encode_header(7, &mut b);
        encode_record(7, &CMDS[0].parse().unwrap(), &mut b);
        let second = b.len();
        encode_record(9, &CMDS[1].parse().unwrap(), &mut b);
        assert_eq!(
            corruption(scan("w", &b, 7)),
            (second as u64, "record LSN 9, expected 8".into())
        );
    }

    #[test]
    fn valid_frame_with_a_bad_body_is_corruption() {
        let mut b = Vec::new();
        encode_header(1, &mut b);
        let at = b.len();
        encode_record(1, &CMDS[0].parse().unwrap(), &mut b);
        b[at + RECORD_HEADER_LEN + 8] = 0; // op tag 0 is never used
        let crc = record_crc(&b[at..]);
        b[at + 4..at + 8].copy_from_slice(&crc.to_le_bytes());
        let (offset, what) = corruption(scan("w", &b, 1));
        assert_eq!(offset, at as u64);
        assert!(what.contains("unknown op tag 0"), "{what}");
    }
}
