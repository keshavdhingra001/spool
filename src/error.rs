use thiserror::Error;

/// A line of the text command format (D12) that couldn't be parsed.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum ParseError {
    #[error("empty line")]
    Empty,
    #[error("missing time: commands start with `@<ms>`, got `{0}`")]
    MissingTime(String),
    #[error(
        "unknown command `{0}` (expected enqueue, lease, heartbeat, ack, nack, configure, redrive or tick)"
    )]
    UnknownCommand(String),
    #[error("`{command}` takes {expected} arguments, got {got}")]
    WrongArgCount {
        command: &'static str,
        expected: &'static str,
        got: usize,
    },
    #[error("unknown option `{0}` (enqueue takes delay=<ms>)")]
    BadOption(String),
    #[error("invalid {field} `{value}`")]
    BadNumber { field: &'static str, value: String },
    #[error("invalid queue name `{0}` (1-64 characters from A-Z a-z 0-9 _ . -)")]
    BadQueueName(String),
    #[error("invalid payload `{0}` (visible ASCII, %XX escapes, `-` for empty)")]
    BadPayload(String),
}
