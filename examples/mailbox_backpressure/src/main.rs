use spawned_concurrency::MailboxConfig;
use spawned_concurrency::error::ActorError;
use spawned_concurrency::link::Exit;
use spawned_concurrency::message::Message;
use spawned_concurrency::tasks::{Actor, ActorStart, Context, Handler};
use spawned_rt::tasks as rt;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

struct GateCounter {
    gate: Arc<(Mutex<bool>, Condvar)>,
}

struct GatedInc;
impl Message for GatedInc {
    type Result = ();
}

struct Ping;
impl Message for Ping {
    type Result = ();
}

impl Actor for GateCounter {}

impl Handler<GatedInc> for GateCounter {
    async fn handle(&mut self, _msg: GatedInc, _ctx: &Context<Self>) {
        let (lock, cvar) = &*self.gate;
        let mut open = lock.lock().unwrap();
        while !*open {
            open = cvar.wait(open).unwrap();
        }
    }
}

impl Handler<Ping> for GateCounter {
    async fn handle(&mut self, _msg: Ping, _ctx: &Context<Self>) {}
}

fn make_gate_counter() -> (GateCounter, Arc<(Mutex<bool>, Condvar)>) {
    let gate = Arc::new((Mutex::new(false), Condvar::new()));
    (GateCounter { gate: gate.clone() }, gate)
}

fn open_gate(gate: &Arc<(Mutex<bool>, Condvar)>) {
    let (lock, cvar) = &**gate;
    *lock.lock().unwrap() = true;
    cvar.notify_all();
}

fn fail_fast_demo() {
    println!("--- Scenario 1: Fail-fast (bounded capacity 1) ---");
    rt::Runtime::new().unwrap().block_on(async {
        let (counter, gate) = make_gate_counter();
        let actor = counter.start_with_mailbox(MailboxConfig::bounded(1));

        actor.send(GatedInc).unwrap();
        rt::sleep(Duration::from_millis(20)).await;
        actor.send(Ping).unwrap();

        match actor.send(Ping) {
            Err(ActorError::MailboxFull) => println!("  Second queued send rejected: MailboxFull"),
            other => panic!("expected MailboxFull, got {other:?}"),
        }

        open_gate(&gate);
        rt::sleep(Duration::from_millis(50)).await;
        actor.child_handle().stop();
        actor.join().await;
    });
    println!();
}

fn block_demo() {
    println!("--- Scenario 2: Block (bounded_blocking capacity 1) ---");
    rt::Runtime::new().unwrap().block_on(async {
        let (counter, gate) = make_gate_counter();
        let actor = counter.start_with_mailbox(MailboxConfig::bounded_blocking(1));

        actor.send(GatedInc).unwrap();
        rt::sleep(Duration::from_millis(20)).await;
        actor.send(Ping).unwrap();

        let actor2 = actor.clone();
        let join = rt::spawn(async move {
            println!("  Blocked send waiting for mailbox space...");
            actor2.send(Ping).unwrap();
            println!("  Blocked send completed after capacity freed");
        });
        rt::sleep(Duration::from_millis(50)).await;
        assert!(!join.is_finished());

        open_gate(&gate);
        join.await.unwrap();

        actor.child_handle().stop();
        actor.join().await;
    });
    println!();
}

struct TrapSupervisor {
    exits: Arc<Mutex<Vec<Exit>>>,
    gate: Arc<(Mutex<bool>, Condvar)>,
}

struct LinkChild(spawned_concurrency::ChildHandle);
impl Message for LinkChild {
    type Result = ();
}

struct GatedWork;
impl Message for GatedWork {
    type Result = ();
}

struct GetExits;
impl Message for GetExits {
    type Result = Vec<Exit>;
}

impl Actor for TrapSupervisor {
    async fn started(&mut self, ctx: &Context<Self>) {
        ctx.trap_exit(true);
    }

    async fn exit_received(&mut self, exit: Exit, _ctx: &Context<Self>) {
        self.exits.lock().unwrap().push(exit);
    }
}

impl Handler<LinkChild> for TrapSupervisor {
    async fn handle(&mut self, msg: LinkChild, ctx: &Context<Self>) {
        ctx.link(&msg.0);
    }
}

impl Handler<GatedWork> for TrapSupervisor {
    async fn handle(&mut self, _msg: GatedWork, _ctx: &Context<Self>) {
        let (lock, cvar) = &*self.gate;
        let mut open = lock.lock().unwrap();
        while !*open {
            open = cvar.wait(open).unwrap();
        }
    }
}

impl Handler<Ping> for TrapSupervisor {
    async fn handle(&mut self, _msg: Ping, _ctx: &Context<Self>) {}
}

impl Handler<GetExits> for TrapSupervisor {
    async fn handle(&mut self, _msg: GetExits, _ctx: &Context<Self>) -> Vec<Exit> {
        self.exits.lock().unwrap().clone()
    }
}

struct StopChild;
impl Message for StopChild {
    type Result = ();
}

struct Stoppable;
impl Actor for Stoppable {}
impl Handler<StopChild> for Stoppable {
    async fn handle(&mut self, _msg: StopChild, ctx: &Context<Self>) {
        ctx.stop();
    }
}

fn system_bypass_demo() {
    println!("--- Scenario 3: System bypass (Exit when mailbox full) ---");
    rt::Runtime::new().unwrap().block_on(async {
        let gate = Arc::new((Mutex::new(false), Condvar::new()));
        let exits = Arc::new(Mutex::new(Vec::new()));

        let supervisor = TrapSupervisor {
            exits: exits.clone(),
            gate: gate.clone(),
        }
        .start_with_mailbox(MailboxConfig::bounded(1));

        let child = Stoppable.start();
        supervisor
            .request(LinkChild(child.child_handle()))
            .await
            .unwrap();

        supervisor.send(GatedWork).unwrap();
        rt::sleep(Duration::from_millis(20)).await;
        supervisor.send(Ping).unwrap();
        assert!(matches!(
            supervisor.send(Ping),
            Err(ActorError::MailboxFull)
        ));

        child.request(StopChild).await.unwrap();
        open_gate(&gate);
        rt::sleep(Duration::from_millis(100)).await;

        let recorded = supervisor.request(GetExits).await.unwrap();
        println!(
            "  Supervisor received {} Exit message(s) while mailbox was full",
            recorded.len()
        );
        assert_eq!(recorded.len(), 1);

        supervisor.child_handle().stop();
        supervisor.join().await;
    });
    println!();
}

fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    println!("=== Mailbox Backpressure Demo ===\n");
    fail_fast_demo();
    block_demo();
    system_bypass_demo();
    println!("=== Done ===");
}
