//! Scenario files (D20): commands in the text format (D12), each followed by
//! the events it must produce, one `= <event>` line per event, in order. A
//! command with no `=` lines must produce no events. `#` starts a comment line.
//!
//! ```text
//! # a job is leased, then acked
//! @0 enqueue emails send:42
//! = enqueued job=1 queue=emails ready_at=0
//! @1 lease emails 30000
//! = leased job=1 token=1 deadline=30001 attempt=1 payload=send:42
//! ```
//!
//! Every command also runs both checkers (D19), so a scenario fails on a broken
//! invariant even where its expected events say nothing about it.

use crate::check::Checked;
use crate::command::Command;

struct Step {
    line: usize,
    command: Command,
    expected: Vec<String>,
}

/// Run a scenario on a fresh queue. The error names the failing line.
pub fn run(text: &str) -> Result<(), String> {
    let mut queue = Checked::new();
    for step in parse(text)? {
        let got: Vec<String> = queue
            .apply(&step.command)
            .map_err(|e| format!("line {}: `{}`: invariant: {e}", step.line, step.command))?
            .iter()
            .map(ToString::to_string)
            .collect();
        if got != step.expected {
            return Err(format!(
                "line {}: `{}`\n  expected:\n{}\n  got:\n{}",
                step.line,
                step.command,
                indent(&step.expected),
                indent(&got)
            ));
        }
    }
    Ok(())
}

fn parse(text: &str) -> Result<Vec<Step>, String> {
    let mut steps: Vec<Step> = Vec::new();
    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if let Some(event) = line.strip_prefix('=') {
            let step = steps
                .last_mut()
                .ok_or_else(|| format!("line {}: expected event before any command", i + 1))?;
            step.expected.push(event.trim().to_string());
        } else {
            let command = line
                .parse()
                .map_err(|e| format!("line {}: `{line}`: {e}", i + 1))?;
            steps.push(Step {
                line: i + 1,
                command,
                expected: Vec::new(),
            });
        }
    }
    Ok(steps)
}

fn indent(lines: &[String]) -> String {
    if lines.is_empty() {
        return "    (no events)".into();
    }
    lines
        .iter()
        .map(|l| format!("    {l}"))
        .collect::<Vec<_>>()
        .join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passes_and_fails() {
        let ok = "# c\n@0 enqueue a x\n= enqueued job=1 queue=a ready_at=0\n\n@1 tick\n";
        assert_eq!(run(ok), Ok(()));

        let missing = "@0 enqueue a x\n";
        assert!(run(missing).unwrap_err().starts_with("line 1:"));

        let extra = "@0 tick\n= acked job=1\n";
        assert!(run(extra).unwrap_err().starts_with("line 1:"));

        let wrong = "@0 enqueue a x\n= enqueued job=1 queue=a ready_at=0\n@1 lease a 5\n= leased job=1 token=2 deadline=6 attempt=1 payload=x\n";
        assert!(run(wrong).unwrap_err().starts_with("line 3:"));

        assert!(
            run("= enqueued job=1")
                .unwrap_err()
                .contains("before any command")
        );
        assert!(run("@0 bogus").unwrap_err().starts_with("line 1:"));
    }
}
