//! The wire protocol (D32): length-prefixed frames carrying ops one way and
//! events the other.
//!
//! ```text
//! frame   = len:u32 kind:u8 id:u64 body          len counts kind, id and body
//! hello      (1)    = version:u32                client -> server, first frame
//! request    (2)    = op (D24, without `at`)     client -> server
//! hello_ok   (0x81) = version:u32                server -> client
//! events     (0x82) = events (D33)               server -> client, one per request
//! error      (0x83) = code:u8 len:u32 message    server -> client, then close
//! ```
//!
//! Replies come in request order and echo the request's id. A frame whose
//! `len` is over [`MAX_FRAME`] is refused before anything is allocated for it.

use std::io;

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::codec::{self, DecodeError, Reader};
use crate::command::{Event, Op};

pub const VERSION: u32 = 1;
/// Largest `len` accepted: 1 MiB (D32).
pub const MAX_FRAME: u32 = 1 << 20;
/// `kind` and `id`: the part of `len` that is not body.
const FRAME_HEAD: u32 = 9;

const HELLO: u8 = 1;
const REQUEST: u8 = 2;
const HELLO_OK: u8 = 0x81;
const EVENTS: u8 = 0x82;
const ERROR: u8 = 0x83;

/// What a client sends.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    Hello { version: u32 },
    Op(Op),
}

/// What the server sends back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Reply {
    HelloOk {
        version: u32,
    },
    Events(Vec<Event>),
    /// The server refused the connection or a frame and closes after this.
    Error {
        code: ErrorCode,
        message: String,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
    /// The client's protocol version is not one the server speaks.
    UnsupportedVersion,
    /// A frame that breaks the protocol: too big, unknown kind, bad body,
    /// or a request before the hello.
    Protocol,
}

impl ErrorCode {
    fn to_u8(self) -> u8 {
        match self {
            ErrorCode::UnsupportedVersion => 1,
            ErrorCode::Protocol => 2,
        }
    }

    fn from_u8(b: u8) -> Result<Self, DecodeError> {
        match b {
            1 => Ok(ErrorCode::UnsupportedVersion),
            2 => Ok(ErrorCode::Protocol),
            tag => Err(DecodeError::UnknownTag {
                what: "error code",
                tag,
            }),
        }
    }
}

/// Why a frame could not be read or understood.
#[derive(Debug, Error)]
pub enum FrameError {
    #[error("connection: {0}")]
    Io(#[from] io::Error),
    #[error("frame length {0} outside 9..={MAX_FRAME}")]
    BadLength(u32),
    #[error("unknown frame kind {0:#x}")]
    UnknownKind(u8),
    #[error("bad frame body: {0}")]
    Body(#[from] DecodeError),
}

/// Append one frame to `out`.
fn put_frame(out: &mut Vec<u8>, kind: u8, id: u64, body: impl FnOnce(&mut Vec<u8>)) {
    let start = out.len();
    out.extend_from_slice(&[0; 4]);
    out.push(kind);
    codec::put_u64(out, id);
    body(out);
    let len = u32::try_from(out.len() - start - 4).expect("frame under 4 GiB");
    out[start..start + 4].copy_from_slice(&len.to_le_bytes());
}

pub fn encode_request(id: u64, req: &Request, out: &mut Vec<u8>) {
    match req {
        Request::Hello { version } => put_frame(out, HELLO, id, |o| codec::put_u32(o, *version)),
        Request::Op(op) => put_frame(out, REQUEST, id, |o| codec::encode_op(op, o)),
    }
}

pub fn encode_reply(id: u64, reply: &Reply, out: &mut Vec<u8>) {
    match reply {
        Reply::HelloOk { version } => put_frame(out, HELLO_OK, id, |o| codec::put_u32(o, *version)),
        Reply::Events(events) => put_frame(out, EVENTS, id, |o| codec::encode_events(events, o)),
        Reply::Error { code, message } => put_frame(out, ERROR, id, |o| {
            o.push(code.to_u8());
            codec::put_u32(o, message.len() as u32);
            o.extend_from_slice(message.as_bytes());
        }),
    }
}

/// One frame as read off the wire, not yet decoded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub kind: u8,
    pub id: u64,
    pub body: Vec<u8>,
}

impl Frame {
    pub fn request(&self) -> Result<Request, FrameError> {
        match self.kind {
            HELLO => {
                let mut r = Reader::new(&self.body);
                let version = r.u32()?;
                r.finish()?;
                Ok(Request::Hello { version })
            }
            REQUEST => Ok(Request::Op(codec::decode_op(&self.body)?)),
            kind => Err(FrameError::UnknownKind(kind)),
        }
    }

    pub fn reply(&self) -> Result<Reply, FrameError> {
        let mut r = Reader::new(&self.body);
        let reply = match self.kind {
            HELLO_OK => Reply::HelloOk { version: r.u32()? },
            EVENTS => return Ok(Reply::Events(codec::decode_events(&self.body)?)),
            ERROR => {
                let code = ErrorCode::from_u8(r.u8()?)?;
                let len = r.u32()?;
                let message = String::from_utf8_lossy(r.bytes(len as usize)?).into_owned();
                Reply::Error { code, message }
            }
            kind => return Err(FrameError::UnknownKind(kind)),
        };
        r.finish()?;
        Ok(reply)
    }
}

/// Read one frame. `Ok(None)` is a clean end of stream between frames; an end
/// inside a frame is an error.
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Option<Frame>, FrameError> {
    let mut len = [0; 4];
    // Read the first byte on its own to tell a clean close from a cut frame.
    if r.read(&mut len[..1]).await? == 0 {
        return Ok(None);
    }
    r.read_exact(&mut len[1..]).await?;
    let len = u32::from_le_bytes(len);
    if !(FRAME_HEAD..=MAX_FRAME).contains(&len) {
        return Err(FrameError::BadLength(len));
    }
    let mut buf = vec![0; len as usize];
    r.read_exact(&mut buf).await?;
    let id = u64::from_le_bytes(buf[1..9].try_into().unwrap());
    Ok(Some(Frame {
        kind: buf[0],
        id,
        body: buf.split_off(9),
    }))
}

/// Write already-encoded frames and flush.
pub async fn write_all<W: AsyncWrite + Unpin>(w: &mut W, bytes: &[u8]) -> io::Result<()> {
    w.write_all(bytes).await?;
    w.flush().await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{JobId, Millis, QueueName, Token};

    fn read_one(bytes: &[u8]) -> Result<Option<Frame>, FrameError> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        rt.block_on(read_frame(&mut &bytes[..]))
    }

    #[test]
    fn golden_frames() {
        let mut out = Vec::new();
        encode_request(7, &Request::Hello { version: 1 }, &mut out);
        assert_eq!(
            out,
            [
                13, 0, 0, 0, // len = kind + id + 4-byte body
                HELLO, 7, 0, 0, 0, 0, 0, 0, 0, // id 7
                1, 0, 0, 0, // version 1
            ]
        );
        let mut out = Vec::new();
        let ack = Op::Ack {
            job: JobId(1),
            token: Token(2),
        };
        encode_request(8, &Request::Op(ack.clone()), &mut out);
        assert_eq!(out[..13], [26, 0, 0, 0, REQUEST, 8, 0, 0, 0, 0, 0, 0, 0]);
        let frame = read_one(&out).unwrap().unwrap();
        assert_eq!((frame.kind, frame.id), (REQUEST, 8));
        assert_eq!(frame.request().unwrap(), Request::Op(ack));
    }

    #[test]
    fn every_reply_round_trips() {
        let replies = [
            Reply::HelloOk { version: 1 },
            Reply::Events(vec![]),
            Reply::Events(vec![Event::Empty {
                queue: QueueName::new("q").unwrap(),
            }]),
            Reply::Error {
                code: ErrorCode::UnsupportedVersion,
                message: "speak 1".into(),
            },
            Reply::Error {
                code: ErrorCode::Protocol,
                message: String::new(),
            },
        ];
        for (id, reply) in replies.into_iter().enumerate() {
            let mut out = Vec::new();
            encode_reply(id as u64, &reply, &mut out);
            let frame = read_one(&out).unwrap().unwrap();
            assert_eq!(frame.id, id as u64);
            assert_eq!(frame.reply().unwrap(), reply);
        }
        let mut out = Vec::new();
        encode_request(
            1,
            &Request::Op(Op::Lease {
                queue: QueueName::new("q").unwrap(),
                visibility: Millis(5),
            }),
            &mut out,
        );
        let frame = read_one(&out).unwrap().unwrap();
        assert!(matches!(
            frame.reply(),
            Err(FrameError::UnknownKind(REQUEST))
        ));
    }

    #[test]
    fn refuses_bad_frames() {
        assert!(read_one(&[]).unwrap().is_none(), "clean end of stream");
        let mut hello = Vec::new();
        encode_request(1, &Request::Hello { version: 1 }, &mut hello);
        for len in 1..hello.len() {
            assert!(
                matches!(read_one(&hello[..len]), Err(FrameError::Io(_))),
                "cut at {len}"
            );
        }
        for len in [0, 8, MAX_FRAME + 1, u32::MAX] {
            let mut bytes = len.to_le_bytes().to_vec();
            bytes.extend_from_slice(&[0; 16]);
            assert!(
                matches!(read_one(&bytes), Err(FrameError::BadLength(l)) if l == len),
                "{len}"
            );
        }
        let mut bad = hello.clone();
        bad[4] = 0x42;
        let frame = read_one(&bad).unwrap().unwrap();
        assert!(matches!(
            frame.request(),
            Err(FrameError::UnknownKind(0x42))
        ));
        let mut long = hello.clone();
        long[0] += 1;
        long.push(0);
        let frame = read_one(&long).unwrap().unwrap();
        assert!(matches!(
            frame.request(),
            Err(FrameError::Body(DecodeError::TrailingBytes(1)))
        ));
        let mut bad_op = Vec::new();
        encode_request(1, &Request::Op(Op::Tick), &mut bad_op);
        bad_op[13] = 0;
        let frame = read_one(&bad_op).unwrap().unwrap();
        assert!(matches!(
            frame.request(),
            Err(FrameError::Body(DecodeError::UnknownTag {
                what: "op",
                tag: 0
            }))
        ));
    }

    #[test]
    fn largest_frame_is_accepted() {
        let mut bytes = MAX_FRAME.to_le_bytes().to_vec();
        bytes.push(HELLO);
        bytes.resize(4 + MAX_FRAME as usize, 0);
        let frame = read_one(&bytes).unwrap().unwrap();
        assert_eq!(frame.body.len(), MAX_FRAME as usize - 9);
    }
}
