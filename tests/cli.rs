//! Smoke test of the real binary (D39): `spool serve` as a process, killed
//! with SIGKILL and restarted on the same directory, and `spool sim` (D55) on
//! the queue world and the Raft world.

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use spool::client::Client;
use spool::{DedupKey, Millis, Payload, QueueName};

/// Start `spool serve` on a free port and return it with its address.
fn serve(dir: &Path) -> (Child, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_spool"))
        .args([
            "serve",
            "--data",
            dir.to_str().unwrap(),
            "--listen",
            "127.0.0.1:0",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    let addr = line
        .trim()
        .strip_prefix("listening on ")
        .unwrap_or_else(|| panic!("unexpected first line {line:?}"))
        .to_string();
    (child, addr)
}

#[tokio::test]
async fn kill_9_and_restart() {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("cli_kill_9");
    let _ = std::fs::remove_dir_all(&dir);
    let queue = QueueName::new("q").unwrap();
    let key = || Some(DedupKey::new("order-1").unwrap());

    let (mut server, addr) = serve(&dir);
    let c = Client::connect(&addr).await.unwrap();
    let first = c
        .enqueue(&queue, Payload(b"x".to_vec()), Millis(0), key())
        .await
        .unwrap();
    c.enqueue(&queue, Payload(b"y".to_vec()), Millis(0), None)
        .await
        .unwrap();
    let leased = c.lease(&queue, Millis(600_000)).await.unwrap().unwrap();

    // The directory is locked: a second server on it refuses to start.
    let second = Command::new(env!("CARGO_BIN_EXE_spool"))
        .args([
            "serve",
            "--data",
            dir.to_str().unwrap(),
            "--listen",
            "127.0.0.1:0",
        ])
        .output()
        .unwrap();
    assert_eq!(second.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&second.stderr).contains("cannot serve"));

    // SIGKILL: no shutdown path runs. Everything acknowledged was synced.
    server.kill().unwrap();
    server.wait().unwrap();
    let (mut server, addr) = serve(&dir);
    let c = Client::connect(&addr).await.unwrap();
    let again = c
        .enqueue(&queue, Payload(b"x".to_vec()), Millis(0), key())
        .await
        .unwrap();
    assert_eq!((again.job, again.deduplicated), (first.job, true));
    // Job 1 is still leased under its token; job 2 is the next one.
    let next = c.lease(&queue, Millis(600_000)).await.unwrap().unwrap();
    assert_eq!(next.payload.0, b"y");
    c.ack(leased.lease.job, leased.lease.token).await.unwrap();
    server.kill().unwrap();
    server.wait().unwrap();
}

#[test]
fn sim_replays_a_seed_and_reports_a_failing_one() {
    let sim = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_spool"))
            .arg("sim")
            .args(args)
            .output()
            .unwrap()
    };
    let traced = sim(&["--seed", "4", "--trace"]);
    assert!(traced.status.success());
    let text = String::from_utf8(traced.stdout).unwrap();
    assert!(
        text.contains("start n0") && text.contains("trace hash"),
        "{text}"
    );
    let hash = |t: &str| {
        t.lines()
            .find(|l| l.contains("trace hash"))
            .unwrap()
            .to_string()
    };
    let plain = String::from_utf8(sim(&["--seed", "4"]).stdout).unwrap();
    assert_eq!(
        hash(&text),
        hash(&plain),
        "the trace does not change the run"
    );

    let swept = sim(&["--seeds", "0..20", "--bug", "no-fence"]);
    assert_eq!(swept.status.code(), Some(1));
    let text = String::from_utf8(swept.stdout).unwrap();
    assert!(
        text.contains("failed") && text.contains("replay: cargo run -- sim --seed"),
        "{text}"
    );
}

#[test]
fn sim_raft_replays_a_seed_and_reports_a_planted_bug() {
    let sim = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_spool"))
            .args(["sim", "--raft"])
            .args(args)
            .output()
            .unwrap()
    };
    let traced = sim(&["--seed", "4", "--trace"]);
    assert!(traced.status.success());
    let text = String::from_utf8(traced.stdout).unwrap();
    assert!(
        text.contains("prevote t1") && text.contains("append t") && text.contains("trace hash"),
        "{text}"
    );
    let swept = sim(&["--seeds", "0..20", "--bug", "no-log-truncate"]);
    assert_eq!(swept.status.code(), Some(1));
    let text = String::from_utf8(swept.stdout).unwrap();
    assert!(
        text.contains("failed") && text.contains("replay: cargo run -- sim --raft --seed"),
        "{text}"
    );
    assert_eq!(
        sim(&["--seed", "1", "--bug", "nonsense"]).status.code(),
        Some(2)
    );
}

#[test]
fn sim_cluster_replays_a_seed_and_reports_a_planted_bug() {
    let sim = |args: &[&str]| {
        Command::new(env!("CARGO_BIN_EXE_spool"))
            .args(["sim", "--cluster"])
            .args(args)
            .output()
            .unwrap()
    };
    let traced = sim(&["--seed", "2", "--trace"]);
    assert!(traced.status.success());
    let text = String::from_utf8(traced.stdout).unwrap();
    assert!(
        text.contains("append t") && text.contains("lease jobs") && text.contains("trace hash"),
        "{text}"
    );
    let swept = sim(&["--seeds", "0..5", "--bug", "no-fence"]);
    assert_eq!(swept.status.code(), Some(1));
    let text = String::from_utf8(swept.stdout).unwrap();
    assert!(
        text.contains("failed") && text.contains("replay: cargo run -- sim --cluster --seed"),
        "{text}"
    );
    assert_eq!(
        sim(&["--seed", "1", "--bug", "nonsense"]).status.code(),
        Some(2)
    );
}
