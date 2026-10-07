//! Interactive REPL over the reference queue, with both checkers (D19) running
//! after every command. `spool --data <dir>` keeps the queue in a data
//! directory instead (M2): every command is logged and synced before its
//! events print, and the next start recovers it.

use std::io::{self, BufRead, Write};
use std::path::PathBuf;

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

fn main() -> io::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut queue = match args.as_slice() {
        [] => {
            println!("spool reference queue, in memory. Type `help`.");
            Backend::Memory(Checked::new())
        }
        [flag, dir] if flag == "--data" => {
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
            eprintln!("usage: spool [--data <dir>]");
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
