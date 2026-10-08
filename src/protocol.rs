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
//! not_leader (0x84) = has_leader:u8 leader:u32   server -> client (version 2, D71)
//! unknown    (0x85)                              server -> client (version 2, D71)
//! peer       (3)    = version:u32 from:u32       replica -> replica, first frame
//! raft       (4)    = message                    replica -> replica, never answered
//!
//! message       = tag:u8 term:u64 fields
//! prevote   (1) = last_index:u64 last_term:u64
//! prevote_reply (2) = granted:u8
//! vote      (3) = last_index:u64 last_term:u64
//! vote_reply (4) = granted:u8
//! append    (5) = prev_index:u64 prev_term:u64 commit:u64 count:u32 entry*
//! append_reply (6) = 0 matched:u64 | 1 prev_index:u64 has_conflict:u8 conflict_term:u64 first_index:u64
//! entry     = term:u64 len:u32 data
//! ```
//!
//! A replica dials each peer and sends only on the connection it dialed; it
//! receives on the ones its peers dialed. Raft tolerates lost messages, so a
//! broken connection is simply redialed. Peer frames may be up to
//! [`MAX_PEER_FRAME`]: an append holds at most 1 MiB of entries beyond its
//! first (D60), and an entry is a batch of at most 1 MiB of ops (D66).
//!
//! Replies come in request order and echo the request's id. Version 2 adds
//! the two replies a replicated node sends (D71); servers accept versions 1
//! and 2, and only a cluster ever sends them. A frame whose
//! `len` is over [`MAX_FRAME`] is refused before anything is allocated for it.

use std::io;

use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::codec::{self, DecodeError, Reader};
use crate::command::{Event, Op};
use crate::raft::{AppendResult, Entry, Message};

/// The newest version; every version from 1 up to it is spoken.
pub const VERSION: u32 = 2;
/// Largest `len` accepted: 1 MiB (D32).
pub const MAX_FRAME: u32 = 1 << 20;
/// Largest `len` accepted on a connection a peer opened.
pub const MAX_PEER_FRAME: u32 = 4 << 20;
/// `kind` and `id`: the part of `len` that is not body.
const FRAME_HEAD: u32 = 9;

const HELLO: u8 = 1;
const REQUEST: u8 = 2;
const PEER: u8 = 3;
const RAFT: u8 = 4;
const HELLO_OK: u8 = 0x81;
const EVENTS: u8 = 0x82;
const ERROR: u8 = 0x83;
const NOT_LEADER: u8 = 0x84;
const UNKNOWN: u8 = 0x85;

/// Whether a server speaks protocol `version`.
pub fn speaks(version: u32) -> bool {
    (1..=VERSION).contains(&version)
}

/// What a client sends.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    Hello {
        version: u32,
    },
    Op(Op),
    /// A replica opening a connection to send Raft messages (M7).
    Peer {
        version: u32,
        from: u32,
    },
    Raft(Message),
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
    /// This node does not lead; ask `leader` (a node id) if known (D71).
    NotLeader {
        leader: Option<u32>,
    },
    /// The request may or may not have applied; it is safe to resend (D72).
    Unknown,
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
        Request::Peer { version, from } => put_frame(out, PEER, id, |o| {
            codec::put_u32(o, *version);
            codec::put_u32(o, *from);
        }),
        Request::Raft(m) => put_frame(out, RAFT, id, |o| encode_message(m, o)),
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
        Reply::NotLeader { leader } => put_frame(out, NOT_LEADER, id, |o| {
            o.push(leader.is_some() as u8);
            codec::put_u32(o, leader.unwrap_or(0));
        }),
        Reply::Unknown => put_frame(out, UNKNOWN, id, |_| {}),
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
            PEER => {
                let mut r = Reader::new(&self.body);
                let version = r.u32()?;
                let from = r.u32()?;
                r.finish()?;
                Ok(Request::Peer { version, from })
            }
            RAFT => Ok(Request::Raft(decode_message(&self.body)?)),
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
            NOT_LEADER => {
                let has = r.u8()?;
                let id = r.u32()?;
                let leader = match has {
                    0 => None,
                    1 => Some(id),
                    _ => return Err(DecodeError::Invalid(format!("leader flag {has}")).into()),
                };
                Reply::NotLeader { leader }
            }
            UNKNOWN => Reply::Unknown,
            kind => return Err(FrameError::UnknownKind(kind)),
        };
        r.finish()?;
        Ok(reply)
    }
}

/// Read one frame. `Ok(None)` is a clean end of stream between frames; an end
/// inside a frame is an error.
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Option<Frame>, FrameError> {
    read_frame_max(r, MAX_FRAME).await
}

/// [`read_frame`] with another cap on `len`, for peer connections.
pub async fn read_frame_max<R: AsyncRead + Unpin>(
    r: &mut R,
    max: u32,
) -> Result<Option<Frame>, FrameError> {
    let mut len = [0; 4];
    // Read the first byte on its own to tell a clean close from a cut frame.
    if r.read(&mut len[..1]).await? == 0 {
        return Ok(None);
    }
    r.read_exact(&mut len[1..]).await?;
    let len = u32::from_le_bytes(len);
    if !(FRAME_HEAD..=max).contains(&len) {
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

/// Decode exactly one frame from `bytes`, length prefix included: how the
/// simulator's datagrams carry frames (D49).
pub fn decode_frame(bytes: &[u8]) -> Result<Frame, FrameError> {
    let (len, rest) = bytes
        .split_first_chunk::<4>()
        .ok_or(DecodeError::Truncated)?;
    let len = u32::from_le_bytes(*len);
    if !(FRAME_HEAD..=MAX_FRAME).contains(&len) {
        return Err(FrameError::BadLength(len));
    }
    let len = len as usize;
    if rest.len() < len {
        return Err(DecodeError::Truncated.into());
    }
    if rest.len() > len {
        return Err(DecodeError::TrailingBytes(rest.len() - len).into());
    }
    Ok(Frame {
        kind: rest[0],
        id: u64::from_le_bytes(rest[1..9].try_into().unwrap()),
        body: rest[9..].to_vec(),
    })
}

const PRE_VOTE: u8 = 1;
const PRE_VOTE_REPLY: u8 = 2;
const VOTE: u8 = 3;
const VOTE_REPLY: u8 = 4;
const APPEND: u8 = 5;
const APPEND_REPLY: u8 = 6;

pub fn encode_message(m: &Message, o: &mut Vec<u8>) {
    let (tag, term) = match m {
        Message::PreVote { term, .. } => (PRE_VOTE, term),
        Message::PreVoteReply { term, .. } => (PRE_VOTE_REPLY, term),
        Message::Vote { term, .. } => (VOTE, term),
        Message::VoteReply { term, .. } => (VOTE_REPLY, term),
        Message::Append { term, .. } => (APPEND, term),
        Message::AppendReply { term, .. } => (APPEND_REPLY, term),
    };
    o.push(tag);
    codec::put_u64(o, *term);
    match m {
        Message::PreVote {
            last_index,
            last_term,
            ..
        }
        | Message::Vote {
            last_index,
            last_term,
            ..
        } => {
            codec::put_u64(o, *last_index);
            codec::put_u64(o, *last_term);
        }
        Message::PreVoteReply { granted, .. } | Message::VoteReply { granted, .. } => {
            o.push(*granted as u8)
        }
        Message::Append {
            prev_index,
            prev_term,
            entries,
            commit,
            ..
        } => {
            codec::put_u64(o, *prev_index);
            codec::put_u64(o, *prev_term);
            codec::put_u64(o, *commit);
            codec::put_u32(o, u32::try_from(entries.len()).expect("under 4G entries"));
            for e in entries {
                codec::put_u64(o, e.term);
                codec::put_u32(o, u32::try_from(e.data.len()).expect("entry under 4 GiB"));
                o.extend_from_slice(&e.data);
            }
        }
        Message::AppendReply { result, .. } => match *result {
            AppendResult::Ok { matched } => {
                o.push(0);
                codec::put_u64(o, matched);
            }
            AppendResult::Reject {
                prev_index,
                conflict_term,
                first_index,
            } => {
                o.push(1);
                codec::put_u64(o, prev_index);
                o.push(conflict_term.is_some() as u8);
                codec::put_u64(o, conflict_term.unwrap_or(0));
                codec::put_u64(o, first_index);
            }
        },
    }
}

pub fn decode_message(bytes: &[u8]) -> Result<Message, DecodeError> {
    let mut r = Reader::new(bytes);
    let tag = r.u8()?;
    let term = r.u64()?;
    let flag = |b: u8, what: &str| match b {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(DecodeError::Invalid(format!("{what} flag {b}"))),
    };
    let m = match tag {
        PRE_VOTE | VOTE => {
            let last_index = r.u64()?;
            let last_term = r.u64()?;
            if tag == PRE_VOTE {
                Message::PreVote {
                    term,
                    last_index,
                    last_term,
                }
            } else {
                Message::Vote {
                    term,
                    last_index,
                    last_term,
                }
            }
        }
        PRE_VOTE_REPLY => Message::PreVoteReply {
            term,
            granted: flag(r.u8()?, "granted")?,
        },
        VOTE_REPLY => Message::VoteReply {
            term,
            granted: flag(r.u8()?, "granted")?,
        },
        APPEND => {
            let prev_index = r.u64()?;
            let prev_term = r.u64()?;
            let commit = r.u64()?;
            let count = r.u32()?;
            let mut entries = Vec::new();
            for _ in 0..count {
                let term = r.u64()?;
                let len = r.u32()?;
                entries.push(Entry {
                    term,
                    data: r.bytes(len as usize)?.to_vec(),
                });
            }
            Message::Append {
                term,
                prev_index,
                prev_term,
                entries,
                commit,
            }
        }
        APPEND_REPLY => {
            let result = match r.u8()? {
                0 => AppendResult::Ok { matched: r.u64()? },
                1 => {
                    let prev_index = r.u64()?;
                    let has = flag(r.u8()?, "conflict")?;
                    let conflict = r.u64()?;
                    AppendResult::Reject {
                        prev_index,
                        conflict_term: has.then_some(conflict),
                        first_index: r.u64()?,
                    }
                }
                tag => {
                    return Err(DecodeError::UnknownTag {
                        what: "append result",
                        tag,
                    });
                }
            };
            Message::AppendReply { term, result }
        }
        tag => {
            return Err(DecodeError::UnknownTag {
                what: "raft message",
                tag,
            });
        }
    };
    r.finish()?;
    Ok(m)
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
            Reply::NotLeader { leader: None },
            Reply::NotLeader { leader: Some(4) },
            Reply::Unknown,
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
    fn decode_frame_takes_exactly_one_whole_frame() {
        let mut hello = Vec::new();
        encode_request(3, &Request::Hello { version: 1 }, &mut hello);
        assert_eq!(
            decode_frame(&hello).unwrap(),
            read_one(&hello).unwrap().unwrap()
        );
        for len in 0..hello.len() {
            assert!(
                matches!(
                    decode_frame(&hello[..len]),
                    Err(FrameError::Body(DecodeError::Truncated))
                ),
                "cut at {len}"
            );
        }
        let mut two = hello.clone();
        two.extend_from_slice(&hello);
        assert!(matches!(
            decode_frame(&two),
            Err(FrameError::Body(DecodeError::TrailingBytes(17)))
        ));
        for len in [0, 8, MAX_FRAME + 1] {
            let mut bytes = len.to_le_bytes().to_vec();
            bytes.extend_from_slice(&[0; 16]);
            assert!(matches!(decode_frame(&bytes), Err(FrameError::BadLength(l)) if l == len));
        }
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
    fn every_raft_message_round_trips() {
        let entries = vec![
            Entry {
                term: 2,
                data: vec![],
            },
            Entry {
                term: 3,
                data: b"batch".to_vec(),
            },
        ];
        let messages = [
            Message::PreVote {
                term: 4,
                last_index: 9,
                last_term: 3,
            },
            Message::PreVoteReply {
                term: 4,
                granted: true,
            },
            Message::Vote {
                term: 5,
                last_index: 1,
                last_term: 1,
            },
            Message::VoteReply {
                term: 5,
                granted: false,
            },
            Message::Append {
                term: 6,
                prev_index: 7,
                prev_term: 2,
                entries,
                commit: 8,
            },
            Message::AppendReply {
                term: 6,
                result: AppendResult::Ok { matched: 9 },
            },
            Message::AppendReply {
                term: 6,
                result: AppendResult::Reject {
                    prev_index: 9,
                    conflict_term: Some(2),
                    first_index: 4,
                },
            },
            Message::AppendReply {
                term: 6,
                result: AppendResult::Reject {
                    prev_index: 9,
                    conflict_term: None,
                    first_index: 4,
                },
            },
        ];
        for m in messages {
            let mut out = Vec::new();
            encode_request(0, &Request::Raft(m.clone()), &mut out);
            let frame = read_one(&out).unwrap().unwrap();
            assert_eq!(frame.request().unwrap(), Request::Raft(m.clone()));
            let mut body = Vec::new();
            encode_message(&m, &mut body);
            for len in 0..body.len() {
                assert!(decode_message(&body[..len]).is_err(), "{m:?} cut at {len}");
            }
        }
        let peer = Request::Peer {
            version: VERSION,
            from: 2,
        };
        let mut out = Vec::new();
        encode_request(0, &peer, &mut out);
        assert_eq!(read_one(&out).unwrap().unwrap().request().unwrap(), peer);
        assert!(matches!(
            decode_message(&[9, 0, 0, 0, 0, 0, 0, 0, 0]),
            Err(DecodeError::UnknownTag { tag: 9, .. })
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
