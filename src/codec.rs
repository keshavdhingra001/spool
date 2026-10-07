//! Binary encoding of commands (D24), used for log records (M2) and, without
//! `at`, for requests on the wire (D32). Little-endian, fields in declaration order:
//!
//! ```text
//! command   = at:u64 op
//! op        = tag:u8 fields
//! enqueue   (1) = queue payload delay:u64              (no key)
//! enqueue   (9) = queue payload delay:u64 key          (with a key, D35)
//! lease     (2) = queue visibility:u64
//! heartbeat (3) = job:u64 token:u64 visibility:u64
//! ack       (4) = job:u64 token:u64
//! nack      (5) = job:u64 token:u64
//! configure (6) = queue max_attempts:u32 backoff_base:u64 backoff_cap:u64
//! redrive   (7) = queue
//! tick      (8)
//! queue     = len:u8 bytes      (validated as a QueueName)
//! payload   = len:u32 bytes
//! key       = len:u8 bytes      (validated as a DedupKey)
//! ```
//!
//! A keyed enqueue has its own tag so that every log written before keys
//! existed decodes unchanged (D35).
//!
//! Tag 0 is never used, so a run of zero bytes never decodes as a command.

use thiserror::Error;

use crate::command::{Command, Op};
use crate::retry::QueueConfig;
use crate::types::{DedupKey, JobId, Millis, Payload, QueueName, Time, Token};

/// Bytes that are not a valid encoding.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum DecodeError {
    #[error("input ends early")]
    Truncated,
    #[error("unknown {what} tag {tag}")]
    UnknownTag { what: &'static str, tag: u8 },
    #[error("invalid queue name {0:?}")]
    BadQueueName(Vec<u8>),
    #[error("invalid dedup key {0:?}")]
    BadKey(Vec<u8>),
    #[error("{0} bytes left over after the value")]
    TrailingBytes(usize),
    #[error("{0}")]
    Invalid(String),
}

const ENQUEUE: u8 = 1;
const LEASE: u8 = 2;
const HEARTBEAT: u8 = 3;
const ACK: u8 = 4;
const NACK: u8 = 5;
const CONFIGURE: u8 = 6;
const REDRIVE: u8 = 7;
const TICK: u8 = 8;
const ENQUEUE_KEYED: u8 = 9;

/// Append the encoding of `cmd` to `out`.
pub fn encode_command(cmd: &Command, out: &mut Vec<u8>) {
    put_u64(out, cmd.at.0);
    encode_op(&cmd.op, out);
}

/// Append the encoding of `op` alone: a request on the wire (D32).
pub fn encode_op(op: &Op, out: &mut Vec<u8>) {
    match op {
        Op::Enqueue {
            queue,
            payload,
            delay,
            key,
        } => {
            out.push(if key.is_some() {
                ENQUEUE_KEYED
            } else {
                ENQUEUE
            });
            put_name(out, queue);
            put_payload(out, payload);
            put_u64(out, delay.0);
            if let Some(key) = key {
                put_key(out, key);
            }
        }
        Op::Lease { queue, visibility } => {
            out.push(LEASE);
            put_name(out, queue);
            put_u64(out, visibility.0);
        }
        Op::Heartbeat {
            job,
            token,
            visibility,
        } => {
            out.push(HEARTBEAT);
            put_u64(out, job.0);
            put_u64(out, token.0);
            put_u64(out, visibility.0);
        }
        Op::Ack { job, token } => {
            out.push(ACK);
            put_u64(out, job.0);
            put_u64(out, token.0);
        }
        Op::Nack { job, token } => {
            out.push(NACK);
            put_u64(out, job.0);
            put_u64(out, token.0);
        }
        Op::Configure { queue, config } => {
            out.push(CONFIGURE);
            put_name(out, queue);
            put_config(out, config);
        }
        Op::Redrive { queue } => {
            out.push(REDRIVE);
            put_name(out, queue);
        }
        Op::Tick => out.push(TICK),
    }
}

/// Decode exactly one command; `bytes` must hold nothing else.
pub fn decode_command(bytes: &[u8]) -> Result<Command, DecodeError> {
    let mut r = Reader::new(bytes);
    let cmd = read_command(&mut r)?;
    r.finish()?;
    Ok(cmd)
}

/// Decode exactly one op; `bytes` must hold nothing else.
pub fn decode_op(bytes: &[u8]) -> Result<Op, DecodeError> {
    let mut r = Reader::new(bytes);
    let op = r.op()?;
    r.finish()?;
    Ok(op)
}

fn read_command(r: &mut Reader) -> Result<Command, DecodeError> {
    let at = Time(r.u64()?);
    let op = r.op()?;
    Ok(Command { at, op })
}

fn read_op(r: &mut Reader) -> Result<Op, DecodeError> {
    let op = match r.u8()? {
        tag @ (ENQUEUE | ENQUEUE_KEYED) => Op::Enqueue {
            queue: r.name()?,
            payload: r.payload()?,
            delay: Millis(r.u64()?),
            key: if tag == ENQUEUE_KEYED {
                Some(r.key()?)
            } else {
                None
            },
        },
        LEASE => Op::Lease {
            queue: r.name()?,
            visibility: Millis(r.u64()?),
        },
        HEARTBEAT => Op::Heartbeat {
            job: JobId(r.u64()?),
            token: Token(r.u64()?),
            visibility: Millis(r.u64()?),
        },
        ACK => Op::Ack {
            job: JobId(r.u64()?),
            token: Token(r.u64()?),
        },
        NACK => Op::Nack {
            job: JobId(r.u64()?),
            token: Token(r.u64()?),
        },
        CONFIGURE => Op::Configure {
            queue: r.name()?,
            config: r.config()?,
        },
        REDRIVE => Op::Redrive { queue: r.name()? },
        TICK => Op::Tick,
        tag => return Err(DecodeError::UnknownTag { what: "op", tag }),
    };
    Ok(op)
}

pub(crate) fn put_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

pub(crate) fn put_name(out: &mut Vec<u8>, name: &QueueName) {
    // QueueName::new caps names at 64 bytes, so the length fits a u8.
    out.push(name.as_str().len() as u8);
    out.extend_from_slice(name.as_str().as_bytes());
}

pub(crate) fn put_key(out: &mut Vec<u8>, key: &DedupKey) {
    // DedupKey::new caps keys at 128 bytes, so the length fits a u8.
    out.push(key.as_str().len() as u8);
    out.extend_from_slice(key.as_str().as_bytes());
}

pub(crate) fn put_payload(out: &mut Vec<u8>, payload: &Payload) {
    let len = u32::try_from(payload.0.len()).expect("payload under 4 GiB");
    put_u32(out, len);
    out.extend_from_slice(&payload.0);
}

pub(crate) fn put_config(out: &mut Vec<u8>, c: &QueueConfig) {
    put_u32(out, c.max_attempts);
    put_u64(out, c.backoff_base.0);
    put_u64(out, c.backoff_cap.0);
}

/// A cursor over a byte slice. Every read checks the length first, so
/// malformed input is an error, never a panic.
pub(crate) struct Reader<'a> {
    buf: &'a [u8],
}

impl<'a> Reader<'a> {
    pub(crate) fn new(buf: &'a [u8]) -> Self {
        Reader { buf }
    }

    pub(crate) fn bytes(&mut self, n: usize) -> Result<&'a [u8], DecodeError> {
        if self.buf.len() < n {
            return Err(DecodeError::Truncated);
        }
        let (head, rest) = self.buf.split_at(n);
        self.buf = rest;
        Ok(head)
    }

    pub(crate) fn u8(&mut self) -> Result<u8, DecodeError> {
        Ok(self.bytes(1)?[0])
    }

    pub(crate) fn u32(&mut self) -> Result<u32, DecodeError> {
        Ok(u32::from_le_bytes(self.bytes(4)?.try_into().unwrap()))
    }

    pub(crate) fn u64(&mut self) -> Result<u64, DecodeError> {
        Ok(u64::from_le_bytes(self.bytes(8)?.try_into().unwrap()))
    }

    pub(crate) fn name(&mut self) -> Result<QueueName, DecodeError> {
        let len = self.u8()?;
        let bytes = self.bytes(len.into())?;
        std::str::from_utf8(bytes)
            .ok()
            .and_then(|s| QueueName::new(s).ok())
            .ok_or_else(|| DecodeError::BadQueueName(bytes.to_vec()))
    }

    pub(crate) fn key(&mut self) -> Result<DedupKey, DecodeError> {
        let len = self.u8()?;
        let bytes = self.bytes(len.into())?;
        std::str::from_utf8(bytes)
            .ok()
            .and_then(|s| DedupKey::new(s).ok())
            .ok_or_else(|| DecodeError::BadKey(bytes.to_vec()))
    }

    pub(crate) fn op(&mut self) -> Result<Op, DecodeError> {
        read_op(self)
    }

    pub(crate) fn payload(&mut self) -> Result<Payload, DecodeError> {
        let len = self.u32()?;
        Ok(Payload(self.bytes(len as usize)?.to_vec()))
    }

    pub(crate) fn config(&mut self) -> Result<QueueConfig, DecodeError> {
        Ok(QueueConfig {
            max_attempts: self.u32()?,
            backoff_base: Millis(self.u64()?),
            backoff_cap: Millis(self.u64()?),
        })
    }

    /// Fail unless every byte was consumed.
    pub(crate) fn finish(&self) -> Result<(), DecodeError> {
        match self.buf.len() {
            0 => Ok(()),
            n => Err(DecodeError::TrailingBytes(n)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode(line: &str) -> Vec<u8> {
        let mut out = Vec::new();
        encode_command(&line.parse().unwrap(), &mut out);
        out
    }

    #[test]
    fn golden_bytes() {
        // Pinned: changing these bytes breaks every existing log (D24).
        assert_eq!(
            encode("@258 enqueue q ab delay=3"),
            [
                2, 1, 0, 0, 0, 0, 0, 0, // at = 258
                ENQUEUE, 1, b'q', // queue "q"
                2, 0, 0, 0, b'a', b'b', // payload "ab"
                3, 0, 0, 0, 0, 0, 0, 0, // delay = 3
            ]
        );
        assert_eq!(encode("@1 tick"), [1, 0, 0, 0, 0, 0, 0, 0, TICK]);
        assert_eq!(
            encode("@0 enqueue q - key=k7"),
            [
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0, // at = 0
                ENQUEUE_KEYED,
                1,
                b'q', // queue "q"
                0,
                0,
                0,
                0, // empty payload
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0, // delay = 0
                2,
                b'k',
                b'7', // key "k7"
            ]
        );
    }

    #[test]
    fn op_alone_is_the_command_without_at() {
        let line = "@9 enqueue q ab delay=3 key=x";
        let cmd: Command = line.parse().unwrap();
        let mut op = Vec::new();
        encode_op(&cmd.op, &mut op);
        assert_eq!(op, encode(line)[8..]);
        assert_eq!(decode_op(&op), Ok(cmd.op));
        op.push(0);
        assert_eq!(decode_op(&op), Err(DecodeError::TrailingBytes(1)));
    }

    #[test]
    fn every_op_round_trips() {
        for line in [
            "@0 enqueue emails - delay=0",
            "@0 enqueue emails - key=%25~",
            "@5 enqueue a.b-c_d %00%FF%0A delay=18446744073709551615",
            "@7 lease q 30",
            "@8 heartbeat 3 9 100",
            "@9 ack 3 9",
            "@10 nack 4 11",
            "@11 configure q 3 100 1000",
            "@12 redrive q",
            "@18446744073709551615 tick",
        ] {
            let cmd: Command = line.parse().unwrap();
            assert_eq!(decode_command(&encode(line)), Ok(cmd), "{line}");
        }
    }

    #[test]
    fn every_truncation_fails() {
        let bytes = encode("@11 configure q 3 100 1000");
        for len in 0..bytes.len() {
            assert_eq!(
                decode_command(&bytes[..len]),
                Err(DecodeError::Truncated),
                "{len}"
            );
        }
    }

    #[test]
    fn rejects_bad_input() {
        let mut bytes = encode("@1 tick");
        bytes.push(0);
        assert_eq!(decode_command(&bytes), Err(DecodeError::TrailingBytes(1)));

        for tag in [0, 10, 255] {
            let mut bytes = encode("@1 tick");
            bytes[8] = tag;
            assert_eq!(
                decode_command(&bytes),
                Err(DecodeError::UnknownTag { what: "op", tag })
            );
        }

        let mut bytes = encode("@1 redrive q");
        bytes[10] = b'/';
        assert_eq!(
            decode_command(&bytes),
            Err(DecodeError::BadQueueName(b"/".to_vec()))
        );
        let mut bytes = encode("@1 redrive q");
        bytes[9] = 0;
        bytes.pop();
        assert_eq!(
            decode_command(&bytes),
            Err(DecodeError::BadQueueName(Vec::new()))
        );

        let mut bytes = encode("@1 enqueue q - key=k");
        *bytes.last_mut().unwrap() = b' ';
        assert_eq!(
            decode_command(&bytes),
            Err(DecodeError::BadKey(b" ".to_vec()))
        );
        let mut bytes = encode("@1 enqueue q - key=k");
        bytes.truncate(bytes.len() - 2);
        bytes.push(0);
        assert_eq!(decode_command(&bytes), Err(DecodeError::BadKey(Vec::new())));
    }
}
