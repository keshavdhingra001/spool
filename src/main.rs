//! Interactive REPL over the reference queue, with both checkers (D19) running
//! after every command. `spool --data <dir>` keeps the queue in a data
//! directory instead (M2): every command is logged and synced before its
//! events print, and the next start recovers it.
//!
//! `spool serve --data <dir> [--listen <addr>]` serves the durable queue over
//! TCP (M3); `spool connect <addr>` is a REPL against a running server, where
//! commands are typed without `@<ms>` because the server stamps the time (D31).
//!
//! `spool sim --seed <n> [--trace]` runs one seed of the deterministic
//! simulation (M5) and `spool sim --seeds <a>..<b>` sweeps a range (D55);
//! `--raft` runs the Raft world (M6) and `--cluster` the replicated queue
//! (M7) instead of the queue world.

use std::io::{self, BufRead, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use spool::client::Client;
use spool::server::{Server, ServerOptions};

use spool::storage::FileStorage;
use spool::{Checked, Command, Durable, Event, Options, ReferenceQueue};

/// Where commands go: memory with both checkers, or a data directory.
enum Backend {
    Memory(Checked),
    Disk(Durable<FileStorage>),
}

impl Backend {
    fn queue(&self) -> &ReferenceQueue {
        match self {
            Backend::Memory(c) => &c.queue,
            Backend::Disk(d) => d.queue(),
        }
    }

    fn apply(&mut self, cmd: &Command) -> Result<Vec<Event>, String> {
        match self {
            Backend::Memory(c) => c
                .apply(cmd)
                .map(<[Event]>::to_vec)
                .map_err(|e| format!("INVARIANT VIOLATED: {e}")),
            Backend::Disk(d) => {
                let events = d.apply(cmd).map_err(|e| format!("error: {e}"))?.to_vec();
                d.queue()
                    .check_invariants()
                    .map_err(|e| format!("INVARIANT VIOLATED: {e}"))?;
                Ok(events)
            }
        }
    }
}

const DEFAULT_LISTEN: &str = "127.0.0.1:7878";

const HELP: &str = "\
commands (every queue command starts with its logical time in ms):
  @<ms> enqueue   <queue> <payload> [delay=<ms>] [key=<key>]
                  payload: visible ASCII, %XX escapes, - for empty; key: dedup key (5 min)
  @<ms> lease     <queue> <visibility_ms>
  @<ms> heartbeat <job> <token> <visibility_ms>
  @<ms> ack       <job> <token>
  @<ms> nack      <job> <token>
  @<ms> complete  <job> <token> <result>      ack and keep the result for 5 min
  @<ms> result    <job>
  @<ms> configure <queue> <max_attempts> <backoff_base_ms> <backoff_cap_ms>
  @<ms> redrive   <queue>
  @<ms> tick
  jobs              list live jobs
  run <file>        run a scenario file on a fresh in-memory queue
  snapshot          write a snapshot now (with --data)
  help | quit
start with `spool --data <dir>` to keep the queue on disk";

const USAGE: &str = "\
usage: spool                                   REPL, in memory
       spool --data <dir>                      REPL, durable
       spool serve --data <dir> [--listen <addr>]   serve over TCP (default 127.0.0.1:7878)
       spool connect <addr>                    REPL against a server
       spool sim --seed <n> [--trace] [--bug <bug>]     run one simulation seed
       spool sim --seeds <a>..<b> [--bug <bug>]         sweep seeds, stop at the first failure
                                               bugs to plant (D54): no-fence, no-dedup-key
       spool sim --raft ...                    the same on the Raft world (M6); bugs (D64):
                                               vote-not-persisted, commit-old-term,
                                               no-log-truncate, stale-term-accept
       spool sim --cluster ...                 the queue on Raft (M7); bugs: the queue's and
                                               reply-before-commit, ignore-term-on-reply (D73)";

fn usage() -> ! {
    eprintln!("{USAGE}");
    std::process::exit(2);
}

/// `spool sim`: replay one seed or sweep a range (D55), of the queue world
/// (M5) or, with `--raft`, of the Raft world (M6). Exits 1 on a failure.
fn sim(args: &[&str]) -> ! {
    use spool::sim::{cluster, queue, raft};
    let mut seeds = None;
    let mut trace = false;
    let mut on_raft = false;
    let mut on_cluster = false;
    let mut bug: Option<&str> = None;
    let mut it = args.iter();
    while let Some(&arg) = it.next() {
        match (arg, it.clone().next()) {
            ("--seed", Some(n)) => {
                let n = n.parse().unwrap_or_else(|_| usage());
                seeds = Some(n..n + 1);
                it.next();
            }
            ("--seeds", Some(range)) => {
                let (a, b) = range.split_once("..").unwrap_or_else(|| usage());
                let a: u64 = a.parse().unwrap_or_else(|_| usage());
                let b: u64 = b.parse().unwrap_or_else(|_| usage());
                seeds = Some(a..b);
                it.next();
            }
            ("--trace", _) => trace = true,
            ("--raft", _) => on_raft = true,
            ("--cluster", _) => on_cluster = true,
            ("--bug", Some(name)) => {
                bug = Some(name);
                it.next();
            }
            _ => usage(),
        }
    }
    let seeds = seeds.unwrap_or_else(|| usage());
    let single = seeds.end - seeds.start == 1;
    let started = std::time::Instant::now();
    let mut runs = 0;
    if on_raft {
        use spool::raft::Bug;
        let bug = bug.map(|b| match b {
            "vote-not-persisted" => Bug::VoteNotPersisted,
            "commit-old-term" => Bug::CommitOldTerm,
            "no-log-truncate" => Bug::NoLogTruncate,
            "stale-term-accept" => Bug::StaleTermAccept,
            _ => usage(),
        });
        let mut total = raft::Coverage::default();
        for seed in seeds {
            let mut options = raft::Options::new(seed);
            options.bug = bug;
            options.trace = trace && single;
            let result = raft::run(&options);
            let report = match &result {
                Ok(r) => r,
                Err(f) => &*f.report,
            };
            if single {
                for line in &report.trace {
                    println!("{line}");
                }
                println!("seed {seed}: {:?}", report.swarm);
                println!(
                    "  finished after {} ms simulated, trace hash {:016x}",
                    report.finished.0, report.hash
                );
                println!("  {:?}", report.world);
                println!("  {:?}", report.coverage);
            }
            if let Err(f) = result {
                println!("{f}");
                std::process::exit(1);
            }
            total.add(&report.coverage);
            runs += 1;
        }
        if !single {
            println!("{runs} seeds passed in {:.1?}", started.elapsed());
            println!("  {total:?}");
        }
        std::process::exit(0);
    }
    if on_cluster {
        use spool::replica::Bug;
        let mut options = cluster::Options::new(0);
        match bug {
            None => {}
            Some("no-fence") => options.queue_bug = Some(queue::Bug::NoFence),
            Some("no-dedup-key") => options.queue_bug = Some(queue::Bug::NoDedupKey),
            Some("reply-before-commit") => options.bug = Some(Bug::ReplyBeforeCommit),
            Some("ignore-term-on-reply") => options.bug = Some(Bug::IgnoreTermOnReply),
            Some(_) => usage(),
        }
        let mut total = cluster::Coverage::default();
        for seed in seeds {
            options.seed = seed;
            options.trace = trace && single;
            let result = cluster::run(&options);
            let report = match &result {
                Ok(r) => r,
                Err(f) => &*f.report,
            };
            if single {
                for line in &report.trace {
                    println!("{line}");
                }
                println!("seed {seed}: {:?}", report.swarm);
                println!(
                    "  finished after {} ms simulated, trace hash {:016x}",
                    report.finished.0, report.hash
                );
                println!("  {:?}", report.world);
                println!("  {:?}", report.coverage);
            }
            if let Err(f) = result {
                println!("{f}");
                std::process::exit(1);
            }
            total.add(&report.coverage);
            runs += 1;
        }
        if !single {
            println!("{runs} seeds passed in {:.1?}", started.elapsed());
            println!("  {total:?}");
        }
        std::process::exit(0);
    }
    let bug = bug.map(|b| match b {
        "no-fence" => queue::Bug::NoFence,
        "no-dedup-key" => queue::Bug::NoDedupKey,
        _ => usage(),
    });
    let mut total = queue::Coverage::default();
    for seed in seeds {
        let mut options = queue::Options::new(seed);
        options.bug = bug;
        options.trace = trace && single;
        let result = queue::run(&options);
        let report = match &result {
            Ok(r) => r,
            Err(f) => &*f.report,
        };
        if single {
            for line in &report.trace {
                println!("{line}");
            }
            println!("seed {seed}: {:?}", report.swarm);
            println!(
                "  finished after {} ms simulated, trace hash {:016x}",
                report.finished.0, report.hash
            );
            println!("  {:?}", report.world);
            println!("  {:?}", report.coverage);
        }
        if let Err(f) = result {
            println!("{f}");
            std::process::exit(1);
        }
        total.add(&report.coverage);
        runs += 1;
    }
    if !single {
        println!("{runs} seeds passed in {:.1?}", started.elapsed());
        println!("  {total:?}");
    }
    std::process::exit(0);
}

/// Serve until Ctrl-C (exit 0) or until the queue fails (exit 1, D38).
fn serve(dir: &Path, listen: SocketAddr) -> io::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let mut server = match Server::start_dir(dir, listen, ServerOptions::default()).await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("cannot serve {}: {e}", dir.display());
                std::process::exit(1);
            }
        };
        println!("listening on {}", server.local_addr());
        io::stdout().flush()?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                let stopped = server.shutdown().await;
                eprintln!("shut down: {:?}", stopped.stats);
                match stopped.result {
                    Ok(()) => Ok(()),
                    Err(e) => {
                        eprintln!("error: {e}");
                        std::process::exit(1);
                    }
                }
            }
            stopped = server.stopped() => {
                let e = stopped.result.err().map_or("stopped".to_string(), |e| e.to_string());
                eprintln!("fatal: {e}; restart to recover");
                std::process::exit(1);
            }
        }
    })
}

/// A REPL that sends each op to a server and prints the events it caused.
fn connect(addr: &str) -> io::Result<()> {
    let rt = tokio::runtime::Runtime::new()?;
    let client = match rt.block_on(Client::connect(addr)) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("cannot connect to {addr}: {e}");
            std::process::exit(1);
        }
    };
    println!("connected to {addr}. Commands as in `help`, without `@<ms>`.");
    let stdin = io::stdin();
    loop {
        print!("> ");
        io::stdout().flush()?;
        let mut line = String::new();
        if stdin.lock().read_line(&mut line)? == 0 {
            return Ok(());
        }
        match line.trim() {
            "" => continue,
            "quit" | "exit" => return Ok(()),
            "help" => println!("{HELP}"),
            input => match format!("@0 {input}").parse::<Command>() {
                Ok(cmd) => match rt.block_on(client.request(cmd.op)) {
                    Ok(events) if events.is_empty() => println!("(no events)"),
                    Ok(events) => events.iter().for_each(|e| println!("{e}")),
                    Err(e) => {
                        println!("error: {e}");
                        return Ok(());
                    }
                },
                Err(e) => println!("error: {e}"),
            },
        }
    }
}

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let mut queue = match args.as_slice() {
        ["serve", "--data", dir] => return serve(Path::new(dir), DEFAULT_LISTEN.parse().unwrap()),
        ["serve", "--data", dir, "--listen", addr] => match addr.parse() {
            Ok(addr) => return serve(Path::new(dir), addr),
            Err(e) => {
                eprintln!("bad address {addr}: {e}");
                std::process::exit(2);
            }
        },
        ["connect", addr] => return connect(addr),
        ["sim", rest @ ..] => sim(rest),
        [] => {
            println!("spool reference queue, in memory. Type `help`.");
            Backend::Memory(Checked::new())
        }
        ["--data", dir] => {
            let dir = PathBuf::from(dir);
            match Durable::open_dir(&dir, Options::default()) {
                Ok((d, rec)) => {
                    println!(
                        "spool queue in {}: snapshot at LSN {}, {} commands replayed, \
                         {} torn bytes cut. Type `help`.",
                        dir.display(),
                        rec.snapshot_lsn,
                        rec.replayed,
                        rec.truncated
                    );
                    Backend::Disk(d)
                }
                Err(e) => {
                    eprintln!("cannot open {}: {e}", dir.display());
                    std::process::exit(1);
                }
            }
        }
        _ => {
            eprintln!("{USAGE}");
            std::process::exit(2);
        }
    };
    let stdin = io::stdin();
    let mut stdout = io::stdout();
    loop {
        print!("> ");
        stdout.flush()?;
        let mut line = String::new();
        if stdin.lock().read_line(&mut line)? == 0 {
            return Ok(());
        }
        match line.trim() {
            "" => continue,
            "help" => println!("{HELP}"),
            "quit" | "exit" => return Ok(()),
            "snapshot" => match &mut queue {
                Backend::Memory(_) => println!("error: no data directory (start with --data)"),
                Backend::Disk(d) => match d.snapshot() {
                    Ok(()) => println!("snapshot at LSN {}", d.next_lsn() - 1),
                    Err(e) => println!("error: {e}"),
                },
            },
            "jobs" => {
                let c = queue.queue().counts();
                println!(
                    "now={} waiting={} leased={} dead={} acked={}",
                    spool::Queue::now(queue.queue()),
                    c.waiting,
                    c.leased,
                    c.dead,
                    c.acked
                );
                for job in queue.queue().jobs() {
                    println!("  {job}");
                }
                for (job, token, result, expires_at) in queue.queue().results() {
                    println!(
                        "  result job={job} token={token} payload={result} expires_at={expires_at}"
                    );
                }
                for (q, key, job, expires_at) in queue.queue().dedup_keys() {
                    println!("  key={key} queue={q} job={job} expires_at={expires_at}");
                }
            }
            input if input.starts_with("run ") => {
                let path = input["run ".len()..].trim();
                match std::fs::read_to_string(path) {
                    Ok(text) => match spool::scenario::run(&text) {
                        Ok(()) => println!("{path}: ok"),
                        Err(e) => println!("{path}: FAILED {e}"),
                    },
                    Err(e) => println!("error: {path}: {e}"),
                }
            }
            input => match input.parse::<Command>() {
                Ok(cmd) => match queue.apply(&cmd) {
                    Ok(events) if events.is_empty() => println!("(no events)"),
                    Ok(events) => events.iter().for_each(|e| println!("{e}")),
                    Err(e) => println!("{e}"),
                },
                Err(e) => println!("error: {e}"),
            },
        }
    }
}
