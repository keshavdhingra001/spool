//! Core value types. Time is logical (D4): a number carried inside commands, never
//! read from a clock by the queue itself.

use std::fmt;

use crate::error::ParseError;

/// A point in logical time, in milliseconds since an arbitrary epoch chosen by
/// whoever stamps the commands (the server edge, the simulator, a test).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Time(pub u64);

/// A length of logical time in milliseconds.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Millis(pub u64);

impl Time {
    /// `self + d`, saturating at `u64::MAX` so a huge visibility timeout means
    /// "never expires" instead of wrapping around into the past.
    pub fn plus(self, d: Millis) -> Time {
        Time(self.0.saturating_add(d.0))
    }
}

/// Broker-assigned job id, unique for the life of the queue (D10).
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct JobId(pub u64);

/// Fencing token (D5, D10). Every lease gets a token larger than every token
/// issued before it, across all jobs, so "newer lease" is a plain comparison.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Token(pub u64);

/// The right to work on one job until `deadline`. Heartbeat, ack and nack must
/// present `job` together with the current `token`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Lease {
    pub job: JobId,
    pub token: Token,
    pub deadline: Time,
}

/// Name of a queue: 1 to [`QueueName::MAX_LEN`] characters from `[A-Za-z0-9_.-]`.
///
/// Restricted so a name is always one whitespace-free token in the text format
/// (D12) and can later be used in file names and metrics labels without escaping.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueueName(String);

impl QueueName {
    pub const MAX_LEN: usize = 64;

    pub fn new(name: &str) -> Result<QueueName, ParseError> {
        let valid_char = |c: char| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-');
        if name.is_empty() || name.len() > Self::MAX_LEN || !name.chars().all(valid_char) {
            return Err(ParseError::BadQueueName(name.to_string()));
        }
        Ok(QueueName(name.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Deduplication key of an enqueue (D35): 1 to [`DedupKey::MAX_LEN`] visible
/// ASCII characters, so it is one whitespace-free token in the text format.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DedupKey(String);

impl DedupKey {
    pub const MAX_LEN: usize = 128;

    pub fn new(key: &str) -> Result<DedupKey, ParseError> {
        if key.is_empty() || key.len() > Self::MAX_LEN || !key.bytes().all(|b| b.is_ascii_graphic())
        {
            return Err(ParseError::BadKey(key.to_string()));
        }
        Ok(DedupKey(key.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// The job's body: opaque bytes that only producers and workers interpret.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Default)]
pub struct Payload(pub Vec<u8>);

impl Payload {
    /// Decode the text form printed by `Display` (D12): bytes from `!` to `~`
    /// except `%` stand for themselves, `%XX` is one byte in hex, and a lone `-`
    /// is the empty payload.
    pub fn parse(s: &str) -> Result<Payload, ParseError> {
        if s == "-" {
            return Ok(Payload(Vec::new()));
        }
        let bad = || ParseError::BadPayload(s.to_string());
        let bytes = s.as_bytes();
        let mut out = Vec::with_capacity(bytes.len());
        let mut i = 0;
        while i < bytes.len() {
            match bytes[i] {
                b'%' => {
                    let (hi, lo) = match bytes.get(i + 1..i + 3) {
                        Some(&[hi, lo]) => (
                            hex_digit(hi).ok_or_else(bad)?,
                            hex_digit(lo).ok_or_else(bad)?,
                        ),
                        _ => return Err(bad()),
                    };
                    out.push(hi << 4 | lo);
                    i += 3;
                }
                b if is_literal(b) => {
                    out.push(b);
                    i += 1;
                }
                _ => return Err(bad()),
            }
        }
        Ok(Payload(out))
    }
}

/// Value of one hex digit. Hand-rolled because `u8::from_str_radix` accepts a
/// leading `+`, which would make `%+1` a valid escape.
fn hex_digit(b: u8) -> Option<u8> {
    (b as char).to_digit(16).map(|d| d as u8)
}

/// Bytes printed as themselves in the payload text form: visible ASCII except `%`.
fn is_literal(b: u8) -> bool {
    b.is_ascii_graphic() && b != b'%'
}

impl fmt::Display for Payload {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0.as_slice() {
            [] => f.write_str("-"),
            // A one-byte payload "-" would print as the empty payload, so escape it.
            [b'-'] => f.write_str("%2D"),
            bytes => {
                for &b in bytes {
                    if is_literal(b) {
                        write!(f, "{}", b as char)?;
                    } else {
                        write!(f, "%{b:02X}")?;
                    }
                }
                Ok(())
            }
        }
    }
}

impl fmt::Display for Time {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Millis {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for JobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl fmt::Display for DedupKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl fmt::Display for QueueName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn time_plus_saturates() {
        assert_eq!(Time(10).plus(Millis(5)), Time(15));
        assert_eq!(Time(u64::MAX - 1).plus(Millis(5)), Time(u64::MAX));
    }

    #[test]
    fn queue_name_rules() {
        for ok in [
            "a",
            "emails",
            "img.resize-v2_hi",
            &"x".repeat(QueueName::MAX_LEN),
        ] {
            assert_eq!(QueueName::new(ok).unwrap().as_str(), ok);
        }
        for bad in [
            "",
            "has space",
            "slash/es",
            "é",
            &"x".repeat(QueueName::MAX_LEN + 1),
        ] {
            assert_eq!(
                QueueName::new(bad),
                Err(ParseError::BadQueueName(bad.to_string()))
            );
        }
    }

    #[test]
    fn dedup_key_rules() {
        for ok in [
            "k",
            "order-42:charge",
            "a=b%c/d",
            &"x".repeat(DedupKey::MAX_LEN),
        ] {
            assert_eq!(DedupKey::new(ok).unwrap().as_str(), ok);
        }
        for bad in [
            "",
            "has space",
            "tab\t",
            "é",
            &"x".repeat(DedupKey::MAX_LEN + 1),
        ] {
            assert_eq!(DedupKey::new(bad), Err(ParseError::BadKey(bad.to_string())));
        }
    }

    #[test]
    fn payload_text_form() {
        let cases: [(&[u8], &str); 6] = [
            (b"", "-"),
            (b"-", "%2D"),
            (b"--", "--"),
            (b"send:42", "send:42"),
            (b"a b%c", "a%20b%25c"),
            (&[0x00, 0xFF, b'\n'], "%00%FF%0A"),
        ];
        for (bytes, text) in cases {
            let p = Payload(bytes.to_vec());
            assert_eq!(p.to_string(), text);
            assert_eq!(Payload::parse(text), Ok(p));
        }
        // Lowercase hex is accepted on input; output is always uppercase.
        assert_eq!(Payload::parse("%2d"), Ok(Payload(b"-".to_vec())));
    }

    #[test]
    fn payload_rejects_bad_escapes() {
        for bad in ["%", "%4", "%zz", "%+1", "%é", "é", "tab\there"] {
            assert_eq!(
                Payload::parse(bad),
                Err(ParseError::BadPayload(bad.to_string())),
                "{bad:?}"
            );
        }
    }
}
