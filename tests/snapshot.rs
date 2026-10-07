//! Snapshots (D27) over every scenario file: after every command, the queue is
//! written out and read back, and the copy must produce the same events as the
//! original for the rest of the file and end in the same state.

use std::fs;

use spool::{Command, Event, Queue, ReferenceQueue, Snapshot};

fn commands(text: &str) -> Vec<Command> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#') && !l.starts_with('='))
        .map(|l| l.parse().unwrap())
        .collect()
}

fn encode(q: &ReferenceQueue) -> Vec<u8> {
    let mut out = Vec::new();
    q.encode_state(&mut out);
    out
}

fn run(q: &mut ReferenceQueue, cmds: &[Command]) -> Vec<Event> {
    let mut out = Vec::new();
    for c in cmds {
        q.apply(c, &mut out);
    }
    out
}

#[test]
fn snapshot_anywhere_then_continue() {
    let mut files = 0;
    for entry in fs::read_dir("tests/scenarios").unwrap() {
        let path = entry.unwrap().path();
        let cmds = commands(&fs::read_to_string(&path).unwrap());
        for split in 0..=cmds.len() {
            let mut original = ReferenceQueue::new();
            run(&mut original, &cmds[..split]);
            let bytes = encode(&original);
            let mut copy = ReferenceQueue::decode_state(&bytes)
                .unwrap_or_else(|e| panic!("{}: after {split}: {e}", path.display()));
            assert_eq!(encode(&copy), bytes);
            let rest = &cmds[split..];
            assert_eq!(
                run(&mut copy, rest),
                run(&mut original, rest),
                "{}: snapshot after {split} commands",
                path.display()
            );
            assert_eq!(encode(&copy), encode(&original));
        }
        files += 1;
    }
    assert!(files >= 9);
}
