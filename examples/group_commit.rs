//! Group commit (D30), measured: concurrent enqueues against a server on a
//! real data directory, with batches of up to 256 commands per sync and with
//! one command per sync.
//!
//! cargo run --release --example group_commit [dir]

use std::path::PathBuf;
use std::time::Instant;

use spool::client::Client;
use spool::server::{Server, ServerOptions};
use spool::{Millis, Payload, QueueName};

const CLIENTS: usize = 64;
const PER_CLIENT: usize = 100;

#[tokio::main]
async fn main() {
    let base = std::env::args()
        .nth(1)
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("target/tmp/group_commit"));
    for max_batch in [1, 256] {
        let dir = base.join(format!("batch-{max_batch}"));
        let _ = std::fs::remove_dir_all(&dir);
        let options = ServerOptions {
            max_batch,
            ..ServerOptions::default()
        };
        let server = Server::start_dir(&dir, "127.0.0.1:0".parse().unwrap(), options)
            .await
            .unwrap();
        let addr = server.local_addr();
        let start = Instant::now();
        let mut tasks = tokio::task::JoinSet::new();
        for c in 0..CLIENTS {
            tasks.spawn(async move {
                let client = Client::connect(addr).await.unwrap();
                let queue = QueueName::new("bench").unwrap();
                for i in 0..PER_CLIENT {
                    let payload = Payload(format!("{c}-{i}").into_bytes());
                    client
                        .enqueue(&queue, payload, Millis(0), None)
                        .await
                        .unwrap();
                }
            });
        }
        tasks.join_all().await;
        let elapsed = start.elapsed();
        let stopped = server.shutdown().await;
        stopped.result.unwrap();
        let s = stopped.stats;
        println!(
            "max_batch {max_batch:>3}: {} enqueues from {CLIENTS} clients in {:.2} s = {:.0}/s; \
             {} syncs, {:.1} commands per sync, largest batch {}",
            s.commands,
            elapsed.as_secs_f64(),
            s.commands as f64 / elapsed.as_secs_f64(),
            s.batches,
            s.commands as f64 / s.batches as f64,
            s.largest_batch
        );
    }
}
