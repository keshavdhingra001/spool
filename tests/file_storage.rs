//! The real directory behind the `Storage` trait (D28, D29), and the durable
//! queue on top of it.

use std::path::PathBuf;

use spool::storage::{FileStorage, LOCK, Storage};

fn fresh_dir(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(name);
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

#[test]
fn file_operations() {
    let dir = fresh_dir("file_operations");
    let mut s = FileStorage::open(&dir).unwrap();
    assert_eq!(
        s.list().unwrap(),
        Vec::<String>::new(),
        "LOCK is not listed"
    );
    assert!(dir.join(LOCK).exists());

    s.create("a").unwrap();
    assert!(s.create("a").is_err(), "create refuses an existing file");
    s.append("a", b"hello").unwrap();
    s.append("a", b" world").unwrap();
    s.sync("a").unwrap();
    assert_eq!(s.read("a").unwrap(), b"hello world");

    s.truncate("a", 5).unwrap();
    s.append("a", b"!").unwrap();
    assert_eq!(s.read("a").unwrap(), b"hello!", "appends go to the new end");

    s.create("b.tmp").unwrap();
    s.append("b.tmp", b"new").unwrap();
    s.rename("b.tmp", "a").unwrap();
    s.append("a", b"er").unwrap();
    s.sync_dir().unwrap();
    assert_eq!(s.read("a").unwrap(), b"newer", "rename replaces the target");
    assert_eq!(s.list().unwrap(), ["a"]);

    s.remove("a").unwrap();
    assert!(s.read("a").is_err());
    assert!(s.append("a", b"x").is_err());
}

#[test]
fn one_open_per_directory() {
    let dir = fresh_dir("one_open_per_directory");
    let first = FileStorage::open(&dir).unwrap();
    let err = FileStorage::open(&dir)
        .err()
        .expect("second open must fail");
    assert_eq!(err.kind(), std::io::ErrorKind::WouldBlock);
    drop(first);
    FileStorage::open(&dir).expect("lock is released on drop");
}

#[test]
fn durable_queue_on_real_files() {
    use spool::{Command, Durable, Options, Queue, ReferenceQueue, Snapshot};

    let dir = fresh_dir("durable_queue_on_real_files");
    let cmds: Vec<Command> = [
        "@0 enqueue q a",
        "@0 enqueue q b",
        "@1 lease q 10",
        "@2 ack 1 1",
        "@3 lease q 10",
        "@20 tick",
        "@21 enqueue r c delay=5",
    ]
    .iter()
    .map(|l| l.parse().unwrap())
    .collect();
    let options = Options { snapshot_every: 3 };
    {
        let (mut d, _) = Durable::<_, ReferenceQueue>::open_dir(&dir, options).unwrap();
        for c in &cmds {
            d.apply(c).unwrap();
        }
        let second = Durable::<_, ReferenceQueue>::open_dir(&dir, options);
        assert!(second.is_err(), "the directory is locked while open");
    }
    let (d, rec) = Durable::<_, ReferenceQueue>::open_dir(&dir, options).unwrap();
    assert_eq!((rec.snapshot_lsn, rec.replayed), (6, 1));

    let mut q = ReferenceQueue::new();
    cmds.iter().for_each(|c| q.apply(c, &mut Vec::new()));
    let (mut want, mut got) = (Vec::new(), Vec::new());
    q.encode_state(&mut want);
    d.queue().encode_state(&mut got);
    assert_eq!(got, want);
    let mut names: Vec<String> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    names.sort();
    assert_eq!(
        names,
        [
            "LOCK",
            "snap-00000000000000000006",
            "wal-00000000000000000007"
        ]
    );
}
