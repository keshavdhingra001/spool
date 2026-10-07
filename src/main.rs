//! Interactive REPL over the reference queue, with both checkers (D19) running
//! after every command. `spool --data <dir>` keeps the queue in a data
//! directory instead (M2): every command is logged and synced before its
//! events print, and the next start recovers it.
//!
//! `spool serve --data <dir> [--listen <addr>]` serves the durable queue over
//! TCP (M3); `spool connect <addr>` is a REPL against a running server, where
//! commands are typed without `@<ms>` because the server stamps the time (D31).

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
       spool connect <addr>                    REPL against a server";

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
