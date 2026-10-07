//! Interactive REPL. In M0 it only parses commands; M1 wires in the reference queue.

use std::io::{self, BufRead, Write};

use spool::Command;

const HELP: &str = "\
commands (every command starts with its logical time in ms):
  @<ms> enqueue   <queue> <payload>       payload: visible ASCII, %XX escapes, - for empty
  @<ms> lease     <queue> <visibility_ms>
  @<ms> heartbeat <job> <token> <visibility_ms>
  @<ms> ack       <job> <token>
  @<ms> nack      <job> <token>
  @<ms> tick
  help | quit";

fn main() -> io::Result<()> {
    println!("spool (M0 scaffold: commands are parsed but not applied yet). Type `help`.");
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
            input => match input.parse::<Command>() {
                Ok(cmd) => println!("parsed: {cmd}"),
                Err(e) => println!("error: {e}"),
            },
        }
    }
}
