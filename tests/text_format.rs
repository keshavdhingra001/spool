//! Property test for the text format (D12): every command prints as a line that
//! parses back to the same command.

use proptest::prelude::*;
use spool::{Command, JobId, Millis, Op, Payload, QueueName, Time, Token};

fn queue() -> impl Strategy<Value = QueueName> {
    "[A-Za-z0-9_.-]{1,64}".prop_map(|s| QueueName::new(&s).unwrap())
}

fn op() -> impl Strategy<Value = Op> {
    let job = any::<u64>().prop_map(JobId);
    let token = any::<u64>().prop_map(Token);
    let ms = any::<u64>().prop_map(Millis);
    prop_oneof![
        (queue(), prop::collection::vec(any::<u8>(), 0..40)).prop_map(|(queue, bytes)| {
            Op::Enqueue {
                queue,
                payload: Payload(bytes),
            }
        }),
        (queue(), ms.clone()).prop_map(|(queue, visibility)| Op::Lease { queue, visibility }),
        (job.clone(), token.clone(), ms).prop_map(|(job, token, visibility)| Op::Heartbeat {
            job,
            token,
            visibility
        }),
        (job.clone(), token.clone()).prop_map(|(job, token)| Op::Ack { job, token }),
        (job, token).prop_map(|(job, token)| Op::Nack { job, token }),
        Just(Op::Tick),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    #[test]
    fn command_round_trips(at in any::<u64>(), op in op()) {
        let cmd = Command { at: Time(at), op };
        let line = cmd.to_string();
        prop_assert!(!line.contains('\n'), "one command per line: {line:?}");
        prop_assert_eq!(line.parse::<Command>(), Ok(cmd));
    }

    #[test]
    fn payload_round_trips(bytes in prop::collection::vec(any::<u8>(), 0..64)) {
        let p = Payload(bytes);
        let text = p.to_string();
        prop_assert!(!text.is_empty() && !text.contains(char::is_whitespace));
        prop_assert_eq!(Payload::parse(&text), Ok(p));
    }
}
