use spawned_concurrency::protocol;
use spawned_concurrency::tasks::{Actor, ChildSpec, Context, Handler, Supervisor};
use spawned_concurrency::{RestartIntensity, RestartType, SupervisorStrategy};
use spawned_rt::tasks as rt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Worker that panics on its first `started()` call, then runs normally after the
/// supervisor restarts it.
struct FlakyWorker {
    name: String,
    starts: Arc<AtomicUsize>,
}

impl Actor for FlakyWorker {
    async fn started(&mut self, _ctx: &Context<Self>) {
        let n = self.starts.fetch_add(1, Ordering::SeqCst);
        tracing::info!("[{}] started (generation {})", self.name, n + 1);
        if n == 0 {
            tracing::warn!(
                "[{}] crashing on first start — supervisor should restart me",
                self.name
            );
            panic!("first-start crash from {}", self.name);
        }
    }

    async fn stopped(&mut self, _ctx: &Context<Self>) {
        tracing::info!("[{}] stopped", self.name);
    }
}

/// Stable worker used to show `Transient` restart policy (no restart on normal exit).
struct StableWorker {
    name: String,
}

#[protocol]
trait StableWorkerProtocol: Send + Sync {
    fn ping(&self) -> spawned_concurrency::Response<String>;
}

#[spawned_concurrency::actor(protocol = StableWorkerProtocol)]
impl StableWorker {
    fn new(name: &str) -> Self {
        StableWorker {
            name: name.to_string(),
        }
    }

    #[started]
    async fn started(&mut self, _ctx: &Context<Self>) {
        tracing::info!("[{}] started", self.name);
    }

    #[request_handler]
    async fn handle_ping(
        &mut self,
        _msg: stable_worker_protocol::Ping,
        _ctx: &Context<Self>,
    ) -> String {
        format!("pong from {}", self.name)
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    rt::run(async {
        println!("=== Supervised Workers Demo ===\n");

        // --- Scenario 1: OneForOne restarts a crashing worker ---
        println!("--- Scenario 1: OneForOne restart after panic ---");

        let flaky_starts = Arc::new(AtomicUsize::new(0));

        let sup = Supervisor::builder()
            .strategy(SupervisorStrategy::OneForOne)
            .intensity(RestartIntensity {
                max_restarts: 5,
                within: Duration::from_secs(10),
            })
            .child(ChildSpec::worker(
                "flaky",
                {
                    let starts = flaky_starts.clone();
                    move || FlakyWorker {
                        name: "flaky".into(),
                        starts: starts.clone(),
                    }
                },
                RestartType::Permanent,
            ))
            .child(ChildSpec::worker(
                "stable",
                || StableWorker::new("stable"),
                RestartType::Transient,
            ))
            .start();

        // Wait for the first crash + supervisor restart
        for _ in 0..50 {
            if flaky_starts.load(Ordering::SeqCst) >= 2 {
                break;
            }
            rt::sleep(Duration::from_millis(20)).await;
        }

        println!(
            "  Flaky worker start count: {} (expect 2 — crash then restart)",
            flaky_starts.load(Ordering::SeqCst)
        );
        println!(
            "  Supervisor still running: {}",
            sup.exit_reason().is_none()
        );

        // --- Scenario 2: Transient worker survives supervisor shutdown without restart ---
        println!("\n--- Scenario 2: Graceful supervisor shutdown ---");
        println!("  Stopping supervisor (children receive Shutdown, not restarted)...");

        sup.child_handle().stop();
        sup.join().await;

        println!("  Supervisor stopped cleanly.\n=== Done ===");
    });
}
