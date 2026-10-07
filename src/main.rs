//! Interactive REPL over the reference queue, with both checkers (D19) running
//! after every command.

use std::io::{self, BufRead, Write};

use spool::{Checked, Command};

const HELP: &str = "\
commands (every queue command starts with its logical time in ms):
  @<ms> enqueue   <queue> <payload> [delay=<ms>]   payload: visible ASCII, %XX escapes, - for empty
  @<ms> lease     <queue> <visibility_ms>
  @<ms> heartbeat <job> <token> <visibility_ms>
  @<ms> ack       <job> <token>
  @<ms> nack      <job> <token>
  @<ms> configure <queue> <max_attempts> <backoff_base_ms> <backoff_cap_ms>
  @<ms> redrive   <queue>
  @<ms> tick
  jobs              list live jobs
  run <file>        run a scenario file on a fresh queue
  help | quit";

fn main() -> io::Result<()> {
    println!("spool reference queue. Type `help`.");
    let mut queue = Checked::new();
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
            "jobs" => {
                let c = queue.queue.counts();
                println!(
                    "now={} waiting={} leased={} dead={} acked={}",
                    spool::Queue::now(&queue.queue),
                    c.waiting,
                    c.leased,
                    c.dead,
                    c.acked
                );
                for job in queue.queue.jobs() {
                    println!("  {job}");
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
                    Ok([]) => println!("(no events)"),
                    Ok(events) => events.iter().for_each(|e| println!("{e}")),
                    Err(e) => println!("INVARIANT VIOLATED: {e}"),
                },
                Err(e) => println!("error: {e}"),
            },
        }
    }
}
