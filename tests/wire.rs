//! Round trips for the wire encodings (D32, D33): every event list encodes to
//! bytes that decode back to it, and every request survives a frame.

use proptest::prelude::*;
use spool::codec::{decode_events, encode_events};
use spool::protocol::{Request, encode_request, read_frame};
use spool::{
    DedupKey, Event, JobId, Lease, Millis, Op, Payload, QueueConfig, QueueName, RejectReason,
    ReleaseReason, Time, Token,
};

fn queue() -> impl Strategy<Value = QueueName> {
    "[A-Za-z0-9_.-]{1,64}".prop_map(|s| QueueName::new(&s).unwrap())
}

fn key() -> impl Strategy<Value = DedupKey> {
    "[!-~]{1,128}".prop_map(|s| DedupKey::new(&s).unwrap())
}

fn event() -> impl Strategy<Value = Event> {
    let job = any::<u64>().prop_map(JobId);
    let time = any::<u64>().prop_map(Time);
    let lease = (any::<u64>(), any::<u64>(), any::<u64>()).prop_map(|(j, t, d)| Lease {
        job: JobId(j),
        token: Token(t),
        deadline: Time(d),
    });
    prop_oneof![
        (job.clone(), queue(), time.clone(), prop::option::of(key())).prop_map(
            |(job, queue, ready_at, key)| Event::Enqueued {
                job,
                queue,
                ready_at,
                key
            }
        ),
        (
            lease.clone(),
            any::<u32>(),
            prop::collection::vec(any::<u8>(), 0..40)
        )
            .prop_map(|(lease, attempt, p)| Event::Leased {
                lease,
                attempt,
                payload: Payload(p)
            }),
        queue().prop_map(|queue| Event::Empty { queue }),
        lease.prop_map(|lease| Event::Renewed { lease }),
        job.clone().prop_map(|job| Event::Acked { job }),
        (job.clone(), any::<u64>(), any::<bool>()).prop_map(|(job, t, nack)| Event::Released {
            job,
            token: Token(t),
            reason: if nack {
                ReleaseReason::Nack
            } else {
                ReleaseReason::Expired
            },
        }),
        (job.clone(), time).prop_map(|(job, ready_at)| Event::Retrying { job, ready_at }),
        job.clone().prop_map(|job| Event::DeadLettered { job }),
        job.clone().prop_map(|job| Event::Redriven { job }),
        (queue(), any::<u32>(), any::<u64>(), any::<u64>()).prop_map(|(queue, n, b, c)| {
            Event::Configured {
                queue,
                config: QueueConfig {
                    max_attempts: n,
                    backoff_base: Millis(b),
                    backoff_cap: Millis(c),
                },
            }
        }),
        prop::sample::select(vec![
            RejectReason::UnknownJob,
            RejectReason::NotLeased,
            RejectReason::StaleToken,
            RejectReason::ZeroVisibility,
            RejectReason::BadConfig,
        ])
        .prop_map(|reason| Event::Rejected { reason }),
        (job, queue(), key()).prop_map(|(job, queue, key)| Event::Deduplicated { job, queue, key }),
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2000))]

    #[test]
    fn events_round_trip(events in prop::collection::vec(event(), 0..6)) {
        let mut bytes = Vec::new();
        encode_events(&events, &mut bytes);
        prop_assert_eq!(decode_events(&bytes), Ok(events));
    }

    #[test]
    fn requests_survive_a_frame(id in any::<u64>(), payload in prop::collection::vec(any::<u8>(), 0..64), key in prop::option::of(key())) {
        let req = Request::Op(Op::Enqueue {
            queue: QueueName::new("q").unwrap(),
            payload: Payload(payload),
            delay: Millis(3),
            key,
        });
        let mut bytes = Vec::new();
        encode_request(id, &req, &mut bytes);
        let rt = tokio::runtime::Builder::new_current_thread().build().unwrap();
        let frame = rt.block_on(read_frame(&mut &bytes[..])).unwrap().unwrap();
        prop_assert_eq!(frame.id, id);
        prop_assert_eq!(frame.request().unwrap(), req);
    }
}
