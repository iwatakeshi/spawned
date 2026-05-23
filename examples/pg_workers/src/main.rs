use spawned_concurrency::message::Message;
use spawned_concurrency::tasks::{Actor, ActorStart, Context, Handler, pg};
use spawned_rt::tasks as rt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

const GROUP: &str = "workers";

struct Ping;

impl Message for Ping {
    type Result = ();
}

struct Worker {
    name: String,
    pings: Arc<AtomicUsize>,
}

impl Actor for Worker {
    async fn started(&mut self, ctx: &Context<Self>) {
        pg::join(GROUP, &ctx.actor_ref());
        tracing::info!("[{}] joined group '{}'", self.name, GROUP);
    }

    async fn stopped(&mut self, ctx: &Context<Self>) {
        let _ = pg::leave(GROUP, ctx.id());
        tracing::info!("[{}] left group '{}'", self.name, GROUP);
    }
}

impl Handler<Ping> for Worker {
    async fn handle(&mut self, _msg: Ping, _ctx: &Context<Self>) {
        let n = self.pings.fetch_add(1, Ordering::SeqCst) + 1;
        tracing::info!("[{}] ping #{n}", self.name);
    }
}

fn broadcast_ping() {
    for worker in pg::members::<Worker>(GROUP) {
        worker.send(Ping).expect("send ping");
    }
}

fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    rt::run(async {
        println!("=== Process Groups Demo ===\n");

        let pings = Arc::new(AtomicUsize::new(0));

        let _w1 = Worker {
            name: "alpha".into(),
            pings: pings.clone(),
        }
        .start();
        let _w2 = Worker {
            name: "beta".into(),
            pings: pings.clone(),
        }
        .start();
        let _w3 = Worker {
            name: "gamma".into(),
            pings: pings.clone(),
        }
        .start();

        rt::sleep(std::time::Duration::from_millis(50)).await;

        println!(
            "--- Group members: {} ---",
            pg::members::<Worker>(GROUP).len()
        );
        assert_eq!(pg::members::<Worker>(GROUP).len(), 3);

        println!("--- Broadcast ping to all workers ---");
        broadcast_ping();
        rt::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(pings.load(Ordering::SeqCst), 3);

        println!("\n--- Stop one worker; auto-leave on exit ---");
        let victim = pg::members::<Worker>(GROUP)[0].clone();
        victim.child_handle().stop();
        victim.join().await;
        rt::sleep(std::time::Duration::from_millis(50)).await;

        assert_eq!(pg::members::<Worker>(GROUP).len(), 2);

        println!("--- Broadcast again ---");
        broadcast_ping();
        rt::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(pings.load(Ordering::SeqCst), 5);

        println!("\nDone.");
    });
}
