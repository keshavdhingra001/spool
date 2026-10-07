//! Runs every scenario file in tests/scenarios (D20).

use std::fs;

#[test]
fn scenarios() {
    let mut paths: Vec<_> = fs::read_dir("tests/scenarios")
        .expect("tests/scenarios exists")
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "txt"))
        .collect();
    paths.sort();
    assert!(paths.len() >= 9, "scenario files missing: {paths:?}");
    let failures: Vec<String> = paths
        .iter()
        .filter_map(|p| {
            let text = fs::read_to_string(p).unwrap();
            spool::scenario::run(&text)
                .err()
                .map(|e| format!("{}: {e}", p.display()))
        })
        .collect();
    assert!(failures.is_empty(), "\n{}", failures.join("\n\n"));
}
