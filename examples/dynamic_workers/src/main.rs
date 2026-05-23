use spawned_concurrency::tasks::{
    dynamic_supervisor::ChildSpec, Actor, Context, DynamicSupervisor, DynamicSupervisorApi,
};
use spawned_concurrency::{RestartIntensity, RestartType};
use spawned_rt::tasks as rt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

struct Worker {
    name: String,
    starts: Arc<AtomicUsize>,
}

impl Actor for Worker {
    async fn started(&mut self, _ctx: &Context<Self>) {
        let n = self.starts.fetch_add(1, Ordering::SeqCst);
        tracing::info!("[{}] started (generation {})", self.name, n + 1);
        if n == 0 {
            tracing::warn!("[{}] crashing once — dynamic supervisor should restart", self.name);
            panic!("demo crash from {}", self.name);
        }
    }

    async fn stopped(&mut self, _ctx: &Context<Self>) {
        tracing::info!("[{}] stopped", self.name);
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    rt::run(async {
        println!("=== Dynamic Workers Demo ===\n");

        let starts = Arc::new(AtomicUsize::new(0));

        let sup = DynamicSupervisor::builder()
            .max_children(10)
            .intensity(RestartIntensity {
                max_restarts: 5,
                within: Duration::from_secs(10),
            })
            .start();

        println!("--- Starting 3 workers at runtime ---");
        for name in ["alpha", "beta", "gamma"] {
            sup.start_child(
                ChildSpec::worker(
                    "worker",
                    {
                        let name = name.to_string();
                        let starts = starts.clone();
                        move || Worker {
                            name: name.clone(),
                            starts: starts.clone(),
                        }
                    },
                    RestartType::Permanent,
                ),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        }

        println!("  Active children: {}", sup.count_children().await.unwrap());

        // Wait for alpha's first crash + restart (all workers share one counter in this demo)
        for _ in 0..50 {
            if starts.load(Ordering::SeqCst) >= 4 {
                break;
            }
            rt::sleep(Duration::from_millis(50)).await;
        }
        println!("  Total worker starts after crash/restart: {}", starts.load(Ordering::SeqCst));

        let children = sup.which_children().await.unwrap();
        if let Some(first) = children.first() {
            println!("\n--- Terminating {} ---", first.id);
            sup.terminate_child(first.actor_id).await.unwrap().unwrap();
        }

        println!("  Active children after terminate: {}", sup.count_children().await.unwrap());

        println!("\n--- Shutting down dynamic supervisor ---");
        sup.child_handle().stop();
        sup.join().await;
        println!("=== Done ===");
    });
}
