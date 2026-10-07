//! The real directory behind the `Storage` trait (D28, D29).

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
