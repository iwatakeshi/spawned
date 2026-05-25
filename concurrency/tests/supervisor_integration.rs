//! Integration tests for supervision — exercises the public crate API end-to-end.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use spawned_concurrency::{
    install_supervision_recorder, shutdown_child_async, shutdown_child_blocking, ChildHandle,
    ExitReason, RestartIntensity, RestartType, ShutdownType, SupervisionRecorder,
    SupervisorStrategy,
};

// ---------------------------------------------------------------------------
// Shared tracking helpers
// ---------------------------------------------------------------------------

type StartCounts = Arc<Mutex<HashMap<String, u32>>>;

fn get_count(counts: &StartCounts, id: &str) -> u32 {
    counts
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .get(id)
        .copied()
        .unwrap_or(0)
}

fn wait_until(counts: &StartCounts, id: &str, at_least: u32, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if get_count(counts, id) >= at_least {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    get_count(counts, id) >= at_least
}

fn wait_until_all(counts: &StartCounts, ids: &[&str], at_least: u32, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if ids.iter().all(|id| get_count(counts, id) >= at_least) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    ids.iter().all(|id| get_count(counts, id) >= at_least)
}

struct CountingRecorder {
    restarts: Arc<std::sync::atomic::AtomicU32>,
}

impl SupervisionRecorder for CountingRecorder {
    fn inc_restart(
        &self,
        _supervisor: spawned_concurrency::ActorId,
        _child_id: &str,
        _remote: bool,
    ) {
        self.restarts
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }

    fn inc_meltdown(&self, _supervisor: spawned_concurrency::ActorId) {}

    fn record_remote_spawn(
        &self,
        _placement: &spawned_concurrency::NodeId,
        _duration: Duration,
        _ok: bool,
    ) {
    }

    fn inc_remote_spawn_retry(&self, _placement: &spawned_concurrency::NodeId) {}
}

// ---------------------------------------------------------------------------
// Tasks mode
// ---------------------------------------------------------------------------

mod tasks {
    use super::*;
    use spawned_concurrency::tasks::{Actor, ActorStart, ChildSpec, Context, Handler, Supervisor};
    use spawned_rt::tasks as rt;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    struct CountingWorker {
        id: String,
        counts: StartCounts,
        panic_at: Vec<u32>,
    }

    impl Actor for CountingWorker {
        async fn started(&mut self, _ctx: &Context<Self>) {
            let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            let gen = map.entry(self.id.clone()).or_insert(0);
            let n = *gen;
            *gen += 1;
            drop(map);
            if self.panic_at.contains(&n) {
                panic!("worker {} crashed on start generation {n}", self.id);
            }
        }
    }

    struct AlwaysPanicWorker {
        id: String,
        counts: StartCounts,
    }

    impl Actor for AlwaysPanicWorker {
        async fn started(&mut self, _ctx: &Context<Self>) {
            let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            *map.entry(self.id.clone()).or_insert(0) += 1;
            drop(map);
            panic!("worker {} always crashes", self.id);
        }
    }

    struct NormalExitWorker {
        id: String,
        counts: StartCounts,
    }

    impl Actor for NormalExitWorker {
        async fn started(&mut self, ctx: &Context<Self>) {
            let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            let gen = map.entry(self.id.clone()).or_insert(0);
            let n = *gen;
            *gen += 1;
            drop(map);
            if n == 0 {
                panic!("worker {} crashes once", self.id);
            }
            ctx.stop();
        }
    }

    struct Idler;
    impl Actor for Idler {}

    struct SlowStopper {
        id: String,
        counts: StartCounts,
        stopped_ran: Arc<AtomicBool>,
        delay: Duration,
    }

    impl Actor for SlowStopper {
        async fn started(&mut self, _ctx: &Context<Self>) {
            let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            *map.entry(self.id.clone()).or_insert(0) += 1;
        }

        async fn stopped(&mut self, _ctx: &Context<Self>) {
            self.stopped_ran.store(true, Ordering::SeqCst);
            rt::sleep(self.delay).await;
        }
    }

    struct Block;

    impl spawned_concurrency::message::Message for Block {
        type Result = ();
    }

    /// Holds the mailbox in a long handler so shutdown timeout can escalate before `stopped()`.
    struct StuckHandler {
        id: String,
        counts: StartCounts,
        hold: Duration,
    }

    impl Actor for StuckHandler {
        async fn started(&mut self, ctx: &Context<Self>) {
            let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            *map.entry(self.id.clone()).or_insert(0) += 1;
            drop(map);
            let _ = ctx.send(Block);
        }
    }

    impl Handler<Block> for StuckHandler {
        async fn handle(&mut self, _msg: Block, _ctx: &Context<Self>) {
            rt::sleep(self.hold).await;
        }
    }

    fn worker(
        id: &str,
        counts: StartCounts,
        panic_at: Vec<u32>,
    ) -> impl Fn() -> CountingWorker + Send + Sync + Clone {
        let id = id.to_string();
        move || CountingWorker {
            id: id.clone(),
            counts: counts.clone(),
            panic_at: panic_at.clone(),
        }
    }

    fn run<F: std::future::Future>(f: F) {
        rt::Runtime::new().unwrap().block_on(f);
    }

    #[test]
    fn one_for_all_restarts_all_children_after_one_crashes() {
        run(async {
            let counts: StartCounts = Arc::new(Mutex::new(HashMap::new()));

            let sup = Supervisor::builder()
                .strategy(SupervisorStrategy::OneForAll)
                .intensity(RestartIntensity {
                    max_restarts: 5,
                    within: Duration::from_secs(10),
                })
                .child(ChildSpec::worker(
                    "alpha",
                    worker("alpha", counts.clone(), vec![0]),
                    RestartType::Permanent,
                ))
                .child(ChildSpec::worker(
                    "beta",
                    worker("beta", counts.clone(), vec![]),
                    RestartType::Permanent,
                ))
                .child(ChildSpec::worker(
                    "gamma",
                    worker("gamma", counts.clone(), vec![]),
                    RestartType::Permanent,
                ))
                .start();

            assert!(
                wait_until_all(
                    &counts,
                    &["alpha", "beta", "gamma"],
                    2,
                    Duration::from_secs(2)
                ),
                "alpha={}, beta={}, gamma={}",
                get_count(&counts, "alpha"),
                get_count(&counts, "beta"),
                get_count(&counts, "gamma"),
            );
            assert!(sup.exit_reason().is_none());

            sup.child_handle().stop();
            sup.join().await;
        });
    }

    #[test]
    fn rest_for_one_restarts_only_later_children() {
        run(async {
            let counts: StartCounts = Arc::new(Mutex::new(HashMap::new()));

            let sup = Supervisor::builder()
                .strategy(SupervisorStrategy::RestForOne)
                .intensity(RestartIntensity {
                    max_restarts: 5,
                    within: Duration::from_secs(10),
                })
                .child(ChildSpec::worker(
                    "first",
                    worker("first", counts.clone(), vec![]),
                    RestartType::Permanent,
                ))
                .child(ChildSpec::worker(
                    "middle",
                    worker("middle", counts.clone(), vec![0]),
                    RestartType::Permanent,
                ))
                .child(ChildSpec::worker(
                    "last",
                    worker("last", counts.clone(), vec![]),
                    RestartType::Permanent,
                ))
                .start();

            assert!(
                wait_until(&counts, "middle", 2, Duration::from_secs(2)),
                "middle={}",
                get_count(&counts, "middle")
            );
            assert!(
                wait_until(&counts, "last", 2, Duration::from_secs(2)),
                "last={}",
                get_count(&counts, "last")
            );

            assert_eq!(get_count(&counts, "first"), 1, "first must not restart");
            assert!(sup.exit_reason().is_none());

            sup.child_handle().stop();
            sup.join().await;
        });
    }

    #[test]
    fn meltdown_stops_supervisor_when_intensity_exceeded() {
        run(async {
            let counts: StartCounts = Arc::new(Mutex::new(HashMap::new()));
            let worker_id = "crash-loop";

            let sup = Supervisor::builder()
                .strategy(SupervisorStrategy::OneForOne)
                .intensity(RestartIntensity {
                    max_restarts: 2,
                    within: Duration::from_secs(10),
                })
                .child(ChildSpec::worker(
                    worker_id,
                    {
                        let counts = counts.clone();
                        let worker_id = worker_id.to_string();
                        move || AlwaysPanicWorker {
                            id: worker_id.clone(),
                            counts: counts.clone(),
                        }
                    },
                    RestartType::Permanent,
                ))
                .start();

            // Initial start + 2 restarts before meltdown on the third crash.
            assert!(
                wait_until(&counts, worker_id, 3, Duration::from_secs(2)),
                "starts={}",
                get_count(&counts, worker_id)
            );

            sup.join().await;
            assert!(
                sup.exit_reason().is_some(),
                "supervisor should stop after meltdown"
            );
        });
    }

    #[test]
    fn transient_does_not_restart_after_normal_exit() {
        run(async {
            let counts: StartCounts = Arc::new(Mutex::new(HashMap::new()));
            let worker_id = "transient";

            let sup = Supervisor::builder()
                .strategy(SupervisorStrategy::OneForOne)
                .child(ChildSpec::worker(
                    worker_id,
                    {
                        let counts = counts.clone();
                        let worker_id = worker_id.to_string();
                        move || NormalExitWorker {
                            id: worker_id.clone(),
                            counts: counts.clone(),
                        }
                    },
                    RestartType::Transient,
                ))
                .start();

            assert!(
                wait_until(&counts, worker_id, 2, Duration::from_secs(2)),
                "starts={}",
                get_count(&counts, worker_id)
            );
            rt::sleep(Duration::from_millis(100)).await;
            assert_eq!(
                get_count(&counts, worker_id),
                2,
                "transient child must not restart again after normal exit"
            );
            assert!(sup.exit_reason().is_none());

            sup.child_handle().stop();
            sup.join().await;
        });
    }

    #[test]
    fn one_for_one_restart_increments_supervision_recorder() {
        run(async {
            let counts: StartCounts = Arc::new(Mutex::new(HashMap::new()));
            let restart_counter = Arc::new(std::sync::atomic::AtomicU32::new(0));
            install_supervision_recorder(Arc::new(CountingRecorder {
                restarts: restart_counter.clone(),
            }));

            let worker_id = "recorder_worker";
            let _sup = Supervisor::builder()
                .strategy(SupervisorStrategy::OneForOne)
                .child(ChildSpec::worker(
                    worker_id,
                    worker(worker_id, counts.clone(), vec![0]),
                    RestartType::Permanent,
                ))
                .start();

            assert!(
                wait_until(&counts, worker_id, 2, Duration::from_secs(2)),
                "worker should restart after panic"
            );
            assert!(
                restart_counter.load(std::sync::atomic::Ordering::SeqCst) >= 1,
                "supervision recorder should count restart"
            );
        });
    }

    #[test]
    fn temporary_never_restarts_after_crash() {
        run(async {
            let counts: StartCounts = Arc::new(Mutex::new(HashMap::new()));
            let worker_id = "temporary";

            let sup = Supervisor::builder()
                .strategy(SupervisorStrategy::OneForOne)
                .child(ChildSpec::worker(
                    worker_id,
                    worker(worker_id, counts.clone(), vec![0]),
                    RestartType::Temporary,
                ))
                .start();

            rt::sleep(Duration::from_millis(200)).await;
            assert_eq!(get_count(&counts, worker_id), 1);
            assert!(sup.exit_reason().is_none());

            sup.child_handle().stop();
            sup.join().await;
        });
    }

    #[test]
    fn child_handle_shutdown_and_kill_exit_reasons() {
        run(async {
            let actor = Idler.start();
            let handle: ChildHandle = actor.child_handle();

            handle.shutdown();
            assert_eq!(handle.wait_exit_async().await, ExitReason::Shutdown);

            let actor = Idler.start();
            let handle = actor.child_handle();
            handle.kill();
            assert_eq!(handle.wait_exit_async().await, ExitReason::Kill);
        });
    }

    #[test]
    fn worker_child_spec_defaults_to_otp_shutdown_timeout() {
        let spec = ChildSpec::worker("w", || Idler, RestartType::Permanent);
        assert_eq!(spec.shutdown, ShutdownType::Timeout(Duration::from_secs(5)));
    }

    #[test]
    fn shutdown_timeout_allows_fast_child_to_shutdown_gracefully() {
        run(async {
            let stopped_ran = Arc::new(AtomicBool::new(false));

            let sup = Supervisor::builder()
                .child(
                    ChildSpec::worker(
                        "fast",
                        {
                            let stopped_ran = stopped_ran.clone();
                            move || SlowStopper {
                                id: "fast".into(),
                                counts: Arc::new(Mutex::new(HashMap::new())),
                                stopped_ran: stopped_ran.clone(),
                                delay: Duration::from_millis(10),
                            }
                        },
                        RestartType::Permanent,
                    )
                    .with_shutdown(ShutdownType::Timeout(Duration::from_secs(1))),
                )
                .start();

            rt::sleep(Duration::from_millis(50)).await;
            sup.child_handle().stop();
            sup.join().await;

            assert!(stopped_ran.load(Ordering::SeqCst));
        });
    }

    #[test]
    fn shutdown_timeout_escalates_stuck_handler_to_kill() {
        run(async {
            let actor = StuckHandler {
                id: "stuck".into(),
                counts: Arc::new(Mutex::new(HashMap::new())),
                hold: Duration::from_secs(5),
            }
            .start();
            let handle = actor.child_handle();

            rt::sleep(Duration::from_millis(20)).await;
            let reason =
                shutdown_child_async(&handle, ShutdownType::Timeout(Duration::from_millis(50)))
                    .await;
            assert_eq!(reason, ExitReason::Kill);
        });
    }

    #[test]
    fn supervisor_shutdown_escalates_stuck_child_to_kill() {
        run(async {
            let counts: StartCounts = Arc::new(Mutex::new(HashMap::new()));

            let sup = Supervisor::builder()
                .child(
                    ChildSpec::worker(
                        "stuck",
                        {
                            let counts = counts.clone();
                            move || StuckHandler {
                                id: "stuck".into(),
                                counts: counts.clone(),
                                hold: Duration::from_secs(5),
                            }
                        },
                        RestartType::Permanent,
                    )
                    .with_shutdown(ShutdownType::Timeout(Duration::from_millis(50))),
                )
                .start();

            rt::sleep(Duration::from_millis(20)).await;
            sup.child_handle().stop();
            sup.join().await;
        });
    }

    #[test]
    fn one_for_all_batch_terminate_escalates_stuck_child() {
        run(async {
            let counts: StartCounts = Arc::new(Mutex::new(HashMap::new()));

            let sup = Supervisor::builder()
                .strategy(SupervisorStrategy::OneForAll)
                .intensity(RestartIntensity {
                    max_restarts: 5,
                    within: Duration::from_secs(10),
                })
                .child(
                    ChildSpec::worker(
                        "fast",
                        worker("fast", counts.clone(), vec![0]),
                        RestartType::Permanent,
                    )
                    .with_shutdown(ShutdownType::BrutalKill),
                )
                .child(
                    ChildSpec::worker(
                        "stuck",
                        {
                            let counts = counts.clone();
                            move || StuckHandler {
                                id: "stuck".into(),
                                counts: counts.clone(),
                                hold: Duration::from_millis(150),
                            }
                        },
                        RestartType::Permanent,
                    )
                    .with_shutdown(ShutdownType::Timeout(Duration::from_millis(50))),
                )
                .start();

            assert!(
                wait_until_all(&counts, &["fast", "stuck"], 2, Duration::from_secs(2)),
                "fast={}, stuck={}",
                get_count(&counts, "fast"),
                get_count(&counts, "stuck"),
            );

            sup.child_handle().stop();
            sup.join().await;
        });
    }

    #[test]
    fn one_for_all_batch_terminate_restarts_all_children() {
        run(async {
            let counts: StartCounts = Arc::new(Mutex::new(HashMap::new()));

            let sup = Supervisor::builder()
                .strategy(SupervisorStrategy::OneForAll)
                .intensity(RestartIntensity {
                    max_restarts: 5,
                    within: Duration::from_secs(10),
                })
                .child(ChildSpec::worker(
                    "fast",
                    worker("fast", counts.clone(), vec![0]),
                    RestartType::Permanent,
                ))
                .child(
                    ChildSpec::worker(
                        "slow",
                        {
                            let counts = counts.clone();
                            move || SlowStopper {
                                id: "slow".into(),
                                counts: counts.clone(),
                                stopped_ran: Arc::new(AtomicBool::new(false)),
                                delay: Duration::from_millis(10),
                            }
                        },
                        RestartType::Permanent,
                    )
                    .with_shutdown(ShutdownType::BrutalKill),
                )
                .start();

            assert!(
                wait_until_all(&counts, &["fast", "slow"], 2, Duration::from_secs(3)),
                "fast={}, slow={}",
                get_count(&counts, "fast"),
                get_count(&counts, "slow")
            );

            sup.child_handle().stop();
            sup.join().await;
        });
    }

    struct GatedMailboxWorker {
        gate: Arc<(Mutex<bool>, std::sync::Condvar)>,
        actor_ref: Arc<Mutex<Option<spawned_concurrency::tasks::ActorRef<GatedMailboxWorker>>>>,
        generation: Arc<AtomicUsize>,
    }

    struct GatedInc;
    impl spawned_concurrency::message::Message for GatedInc {
        type Result = ();
    }

    struct MbPing;
    impl spawned_concurrency::message::Message for MbPing {
        type Result = ();
    }

    struct PanicNow;
    impl spawned_concurrency::message::Message for PanicNow {
        type Result = ();
    }

    impl Actor for GatedMailboxWorker {
        async fn started(&mut self, ctx: &Context<Self>) {
            self.generation.fetch_add(1, Ordering::SeqCst);
            *self.actor_ref.lock().unwrap() = Some(ctx.actor_ref());
        }
    }

    impl Handler<GatedInc> for GatedMailboxWorker {
        async fn handle(&mut self, _msg: GatedInc, _ctx: &Context<Self>) {
            let (lock, cvar) = &*self.gate;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = cvar.wait(open).unwrap();
            }
        }
    }

    impl Handler<MbPing> for GatedMailboxWorker {
        async fn handle(&mut self, _msg: MbPing, _ctx: &Context<Self>) {}
    }

    impl Handler<PanicNow> for GatedMailboxWorker {
        async fn handle(&mut self, _msg: PanicNow, _ctx: &Context<Self>) {
            panic!("supervised worker panic");
        }
    }

    fn gated_mailbox_worker(
        gate: Arc<(Mutex<bool>, std::sync::Condvar)>,
        actor_ref: Arc<Mutex<Option<spawned_concurrency::tasks::ActorRef<GatedMailboxWorker>>>>,
        generation: Arc<AtomicUsize>,
    ) -> impl Fn() -> GatedMailboxWorker + Send + Sync + Clone {
        move || GatedMailboxWorker {
            gate: gate.clone(),
            actor_ref: actor_ref.clone(),
            generation: generation.clone(),
        }
    }

    fn wait_for_worker_ref(
        actor_ref: &Arc<Mutex<Option<spawned_concurrency::tasks::ActorRef<GatedMailboxWorker>>>>,
    ) -> spawned_concurrency::tasks::ActorRef<GatedMailboxWorker> {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if let Some(actor) = actor_ref.lock().unwrap().clone() {
                return actor;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        panic!("worker actor ref not published");
    }

    #[test]
    fn supervised_child_honors_child_spec_mailbox() {
        use spawned_concurrency::error::ActorError;
        use spawned_concurrency::MailboxConfig;

        run(async {
            let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
            let worker_ref = Arc::new(Mutex::new(None));
            let generation = Arc::new(AtomicUsize::new(0));

            let sup = Supervisor::builder()
                .child(
                    ChildSpec::worker(
                        "gated",
                        gated_mailbox_worker(gate.clone(), worker_ref.clone(), generation.clone()),
                        RestartType::Permanent,
                    )
                    .with_mailbox(MailboxConfig::bounded(1)),
                )
                .start();

            let actor = wait_for_worker_ref(&worker_ref);
            actor.send(GatedInc).unwrap();
            rt::sleep(Duration::from_millis(20)).await;
            actor.send(MbPing).unwrap();
            assert!(matches!(actor.send(MbPing), Err(ActorError::MailboxFull)));

            {
                let (lock, cvar) = &*gate;
                *lock.lock().unwrap() = true;
                cvar.notify_all();
            }

            sup.child_handle().stop();
            sup.join().await;
        });
    }

    #[test]
    fn supervised_child_restart_preserves_mailbox() {
        use spawned_concurrency::error::ActorError;
        use spawned_concurrency::MailboxConfig;

        run(async {
            let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
            let worker_ref = Arc::new(Mutex::new(None));
            let generation = Arc::new(AtomicUsize::new(0));

            let sup = Supervisor::builder()
                .child(
                    ChildSpec::worker(
                        "gated",
                        gated_mailbox_worker(gate.clone(), worker_ref.clone(), generation.clone()),
                        RestartType::Permanent,
                    )
                    .with_mailbox(MailboxConfig::bounded(1)),
                )
                .start();

            let actor = wait_for_worker_ref(&worker_ref);
            let _ = actor.request(PanicNow).await;
            while generation.load(Ordering::SeqCst) < 2 {
                rt::sleep(Duration::from_millis(10)).await;
            }

            let actor = wait_for_worker_ref(&worker_ref);
            actor.send(GatedInc).unwrap();
            rt::sleep(Duration::from_millis(20)).await;
            actor.send(MbPing).unwrap();
            assert!(matches!(actor.send(MbPing), Err(ActorError::MailboxFull)));

            {
                let (lock, cvar) = &*gate;
                *lock.lock().unwrap() = true;
                cvar.notify_all();
            }

            sup.child_handle().stop();
            sup.join().await;
        });
    }
}

// ---------------------------------------------------------------------------
// Threads mode
// ---------------------------------------------------------------------------

mod threads {
    use super::*;
    use spawned_concurrency::threads::{
        Actor, ActorStart, ChildSpec, Context, Handler, Supervisor,
    };
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::thread;

    struct CountingWorker {
        id: String,
        counts: StartCounts,
        panic_at: Vec<u32>,
    }

    impl Actor for CountingWorker {
        fn started(&mut self, _ctx: &Context<Self>) {
            let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            let gen = map.entry(self.id.clone()).or_insert(0);
            let n = *gen;
            *gen += 1;
            drop(map);
            if self.panic_at.contains(&n) {
                panic!("worker {} crashed on start generation {n}", self.id);
            }
        }
    }

    struct AlwaysPanicWorker {
        id: String,
        counts: StartCounts,
    }

    impl Actor for AlwaysPanicWorker {
        fn started(&mut self, _ctx: &Context<Self>) {
            let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            *map.entry(self.id.clone()).or_insert(0) += 1;
            drop(map);
            panic!("worker {} always crashes", self.id);
        }
    }

    struct NormalExitWorker {
        id: String,
        counts: StartCounts,
    }

    impl Actor for NormalExitWorker {
        fn started(&mut self, ctx: &Context<Self>) {
            let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            let gen = map.entry(self.id.clone()).or_insert(0);
            let n = *gen;
            *gen += 1;
            drop(map);
            if n == 0 {
                panic!("worker {} crashes once", self.id);
            }
            ctx.stop();
        }
    }

    struct SlowStopper {
        id: String,
        counts: StartCounts,
        stopped_ran: Arc<AtomicBool>,
        delay: Duration,
    }

    impl Actor for SlowStopper {
        fn started(&mut self, _ctx: &Context<Self>) {
            let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            *map.entry(self.id.clone()).or_insert(0) += 1;
        }

        fn stopped(&mut self, _ctx: &Context<Self>) {
            self.stopped_ran.store(true, Ordering::SeqCst);
            thread::sleep(self.delay);
        }
    }

    struct Block;

    impl spawned_concurrency::message::Message for Block {
        type Result = ();
    }

    struct StuckHandler {
        id: String,
        counts: StartCounts,
        hold: Duration,
    }

    impl Actor for StuckHandler {
        fn started(&mut self, ctx: &Context<Self>) {
            let mut map = self.counts.lock().unwrap_or_else(|p| p.into_inner());
            *map.entry(self.id.clone()).or_insert(0) += 1;
            drop(map);
            let _ = ctx.send(Block);
        }
    }

    impl Handler<Block> for StuckHandler {
        fn handle(&mut self, _msg: Block, _ctx: &Context<Self>) {
            thread::sleep(self.hold);
        }
    }

    fn worker(
        id: &str,
        counts: StartCounts,
        panic_at: Vec<u32>,
    ) -> impl Fn() -> CountingWorker + Send + Sync + Clone {
        let id = id.to_string();
        move || CountingWorker {
            id: id.clone(),
            counts: counts.clone(),
            panic_at: panic_at.clone(),
        }
    }

    struct Idler;
    impl Actor for Idler {}

    #[test]
    fn worker_child_spec_defaults_to_otp_shutdown_timeout() {
        let spec = ChildSpec::worker("w", || Idler, RestartType::Permanent);
        assert_eq!(spec.shutdown, ShutdownType::Timeout(Duration::from_secs(5)));
    }

    #[test]
    fn one_for_all_restarts_all_children_after_one_crashes() {
        let counts: StartCounts = Arc::new(Mutex::new(HashMap::new()));

        let sup = Supervisor::builder()
            .strategy(SupervisorStrategy::OneForAll)
            .intensity(RestartIntensity {
                max_restarts: 5,
                within: Duration::from_secs(10),
            })
            .child(ChildSpec::worker(
                "alpha",
                worker("alpha", counts.clone(), vec![0]),
                RestartType::Permanent,
            ))
            .child(ChildSpec::worker(
                "beta",
                worker("beta", counts.clone(), vec![]),
                RestartType::Permanent,
            ))
            .child(ChildSpec::worker(
                "gamma",
                worker("gamma", counts.clone(), vec![]),
                RestartType::Permanent,
            ))
            .start();

        assert!(
            wait_until_all(
                &counts,
                &["alpha", "beta", "gamma"],
                2,
                Duration::from_secs(2)
            ),
            "alpha={}, beta={}, gamma={}",
            get_count(&counts, "alpha"),
            get_count(&counts, "beta"),
            get_count(&counts, "gamma"),
        );
        assert!(sup.exit_reason().is_none());

        sup.child_handle().stop();
        sup.join();
    }

    #[test]
    fn rest_for_one_restarts_only_later_children() {
        let counts: StartCounts = Arc::new(Mutex::new(HashMap::new()));

        let sup = Supervisor::builder()
            .strategy(SupervisorStrategy::RestForOne)
            .intensity(RestartIntensity {
                max_restarts: 5,
                within: Duration::from_secs(10),
            })
            .child(ChildSpec::worker(
                "first",
                worker("first", counts.clone(), vec![]),
                RestartType::Permanent,
            ))
            .child(ChildSpec::worker(
                "middle",
                worker("middle", counts.clone(), vec![0]),
                RestartType::Permanent,
            ))
            .child(ChildSpec::worker(
                "last",
                worker("last", counts.clone(), vec![]),
                RestartType::Permanent,
            ))
            .start();

        assert!(
            wait_until(&counts, "middle", 2, Duration::from_secs(2)),
            "middle={}",
            get_count(&counts, "middle")
        );
        assert!(
            wait_until(&counts, "last", 2, Duration::from_secs(2)),
            "last={}",
            get_count(&counts, "last")
        );
        assert_eq!(get_count(&counts, "first"), 1);

        sup.child_handle().stop();
        sup.join();
    }

    #[test]
    fn meltdown_stops_supervisor_when_intensity_exceeded() {
        let counts: StartCounts = Arc::new(Mutex::new(HashMap::new()));
        let worker_id = "crash-loop";

        let sup = Supervisor::builder()
            .strategy(SupervisorStrategy::OneForOne)
            .intensity(RestartIntensity {
                max_restarts: 2,
                within: Duration::from_secs(10),
            })
            .child(ChildSpec::worker(
                worker_id,
                {
                    let counts = counts.clone();
                    let worker_id = worker_id.to_string();
                    move || AlwaysPanicWorker {
                        id: worker_id.clone(),
                        counts: counts.clone(),
                    }
                },
                RestartType::Permanent,
            ))
            .start();

        assert!(
            wait_until(&counts, worker_id, 3, Duration::from_secs(2)),
            "starts={}",
            get_count(&counts, worker_id)
        );

        sup.join();
        assert!(sup.exit_reason().is_some());
    }

    #[test]
    fn transient_does_not_restart_after_normal_exit() {
        let counts: StartCounts = Arc::new(Mutex::new(HashMap::new()));
        let worker_id = "transient";

        let sup = Supervisor::builder()
            .strategy(SupervisorStrategy::OneForOne)
            .child(ChildSpec::worker(
                worker_id,
                {
                    let counts = counts.clone();
                    let worker_id = worker_id.to_string();
                    move || NormalExitWorker {
                        id: worker_id.clone(),
                        counts: counts.clone(),
                    }
                },
                RestartType::Transient,
            ))
            .start();

        assert!(
            wait_until(&counts, worker_id, 2, Duration::from_secs(2)),
            "starts={}",
            get_count(&counts, worker_id)
        );
        thread::sleep(Duration::from_millis(100));
        assert_eq!(get_count(&counts, worker_id), 2);

        sup.child_handle().stop();
        sup.join();
    }

    #[test]
    fn temporary_never_restarts_after_crash() {
        let counts: StartCounts = Arc::new(Mutex::new(HashMap::new()));
        let worker_id = "temporary";

        let sup = Supervisor::builder()
            .strategy(SupervisorStrategy::OneForOne)
            .child(ChildSpec::worker(
                worker_id,
                worker(worker_id, counts.clone(), vec![0]),
                RestartType::Temporary,
            ))
            .start();

        thread::sleep(Duration::from_millis(200));
        assert_eq!(get_count(&counts, worker_id), 1);

        sup.child_handle().stop();
        sup.join();
    }

    #[test]
    fn shutdown_timeout_escalates_stuck_handler_to_kill() {
        let actor = StuckHandler {
            id: "stuck".into(),
            counts: Arc::new(Mutex::new(HashMap::new())),
            hold: Duration::from_secs(5),
        }
        .start();
        let handle = actor.child_handle();

        thread::sleep(Duration::from_millis(20));
        let reason =
            shutdown_child_blocking(&handle, ShutdownType::Timeout(Duration::from_millis(50)));
        assert_eq!(reason, ExitReason::Kill);
    }

    #[test]
    fn supervisor_shutdown_escalates_stuck_child_to_kill() {
        let counts: StartCounts = Arc::new(Mutex::new(HashMap::new()));

        let sup = Supervisor::builder()
            .child(
                ChildSpec::worker(
                    "stuck",
                    {
                        let counts = counts.clone();
                        move || StuckHandler {
                            id: "stuck".into(),
                            counts: counts.clone(),
                            hold: Duration::from_millis(150),
                        }
                    },
                    RestartType::Permanent,
                )
                .with_shutdown(ShutdownType::Timeout(Duration::from_millis(50))),
            )
            .start();

        thread::sleep(Duration::from_millis(20));
        sup.child_handle().stop();
        sup.join();
    }

    #[test]
    fn one_for_all_batch_terminate_escalates_stuck_child() {
        let counts: StartCounts = Arc::new(Mutex::new(HashMap::new()));

        let sup = Supervisor::builder()
            .strategy(SupervisorStrategy::OneForAll)
            .intensity(RestartIntensity {
                max_restarts: 5,
                within: Duration::from_secs(10),
            })
            .child(
                ChildSpec::worker(
                    "fast",
                    worker("fast", counts.clone(), vec![0]),
                    RestartType::Permanent,
                )
                .with_shutdown(ShutdownType::BrutalKill),
            )
            .child(
                ChildSpec::worker(
                    "stuck",
                    {
                        let counts = counts.clone();
                        move || StuckHandler {
                            id: "stuck".into(),
                            counts: counts.clone(),
                            hold: Duration::from_millis(150),
                        }
                    },
                    RestartType::Permanent,
                )
                .with_shutdown(ShutdownType::Timeout(Duration::from_millis(50))),
            )
            .start();

        assert!(
            wait_until_all(&counts, &["fast", "stuck"], 2, Duration::from_secs(2)),
            "fast={}, stuck={}",
            get_count(&counts, "fast"),
            get_count(&counts, "stuck"),
        );

        sup.child_handle().stop();
        sup.join();
    }

    #[test]
    fn shutdown_timeout_allows_fast_child_to_shutdown_gracefully() {
        let stopped_ran = Arc::new(AtomicBool::new(false));

        let sup = Supervisor::builder()
            .child(
                ChildSpec::worker(
                    "fast",
                    {
                        let stopped_ran = stopped_ran.clone();
                        move || SlowStopper {
                            id: "fast".into(),
                            counts: Arc::new(Mutex::new(HashMap::new())),
                            stopped_ran: stopped_ran.clone(),
                            delay: Duration::from_millis(10),
                        }
                    },
                    RestartType::Permanent,
                )
                .with_shutdown(ShutdownType::Timeout(Duration::from_secs(1))),
            )
            .start();

        thread::sleep(Duration::from_millis(50));
        sup.child_handle().stop();
        sup.join();

        assert!(stopped_ran.load(Ordering::SeqCst));
    }

    struct GatedMailboxWorker {
        gate: Arc<(Mutex<bool>, std::sync::Condvar)>,
        actor_ref: Arc<Mutex<Option<spawned_concurrency::threads::ActorRef<GatedMailboxWorker>>>>,
        generation: Arc<AtomicUsize>,
    }

    struct GatedInc;
    impl spawned_concurrency::message::Message for GatedInc {
        type Result = ();
    }

    struct MbPing;
    impl spawned_concurrency::message::Message for MbPing {
        type Result = ();
    }

    struct PanicNow;
    impl spawned_concurrency::message::Message for PanicNow {
        type Result = ();
    }

    impl Actor for GatedMailboxWorker {
        fn started(&mut self, ctx: &Context<Self>) {
            self.generation.fetch_add(1, Ordering::SeqCst);
            *self.actor_ref.lock().unwrap() = Some(ctx.actor_ref());
        }
    }

    impl Handler<GatedInc> for GatedMailboxWorker {
        fn handle(&mut self, _msg: GatedInc, _ctx: &Context<Self>) {
            let (lock, cvar) = &*self.gate;
            let mut open = lock.lock().unwrap();
            while !*open {
                open = cvar.wait(open).unwrap();
            }
        }
    }

    impl Handler<MbPing> for GatedMailboxWorker {
        fn handle(&mut self, _msg: MbPing, _ctx: &Context<Self>) {}
    }

    impl Handler<PanicNow> for GatedMailboxWorker {
        fn handle(&mut self, _msg: PanicNow, _ctx: &Context<Self>) {
            panic!("supervised worker panic");
        }
    }

    fn gated_mailbox_worker(
        gate: Arc<(Mutex<bool>, std::sync::Condvar)>,
        actor_ref: Arc<Mutex<Option<spawned_concurrency::threads::ActorRef<GatedMailboxWorker>>>>,
        generation: Arc<AtomicUsize>,
    ) -> impl Fn() -> GatedMailboxWorker + Send + Sync + Clone {
        move || GatedMailboxWorker {
            gate: gate.clone(),
            actor_ref: actor_ref.clone(),
            generation: generation.clone(),
        }
    }

    fn wait_for_worker_ref(
        actor_ref: &Arc<Mutex<Option<spawned_concurrency::threads::ActorRef<GatedMailboxWorker>>>>,
    ) -> spawned_concurrency::threads::ActorRef<GatedMailboxWorker> {
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while std::time::Instant::now() < deadline {
            if let Some(actor) = actor_ref.lock().unwrap().clone() {
                return actor;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("worker actor ref not published");
    }

    #[test]
    fn supervised_child_honors_child_spec_mailbox() {
        use spawned_concurrency::error::ActorError;
        use spawned_concurrency::MailboxConfig;

        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let worker_ref = Arc::new(Mutex::new(None));
        let generation = Arc::new(AtomicUsize::new(0));

        let sup = Supervisor::builder()
            .child(
                ChildSpec::worker(
                    "gated",
                    gated_mailbox_worker(gate.clone(), worker_ref.clone(), generation.clone()),
                    RestartType::Permanent,
                )
                .with_mailbox(MailboxConfig::bounded(1)),
            )
            .start();

        let actor = wait_for_worker_ref(&worker_ref);
        actor.send(GatedInc).unwrap();
        thread::sleep(Duration::from_millis(20));
        actor.send(MbPing).unwrap();
        assert!(matches!(actor.send(MbPing), Err(ActorError::MailboxFull)));

        {
            let (lock, cvar) = &*gate;
            *lock.lock().unwrap() = true;
            cvar.notify_all();
        }

        sup.child_handle().stop();
        sup.join();
    }

    #[test]
    fn supervised_child_restart_preserves_mailbox() {
        use spawned_concurrency::error::ActorError;
        use spawned_concurrency::MailboxConfig;

        let gate = Arc::new((Mutex::new(false), std::sync::Condvar::new()));
        let worker_ref = Arc::new(Mutex::new(None));
        let generation = Arc::new(AtomicUsize::new(0));

        let sup = Supervisor::builder()
            .child(
                ChildSpec::worker(
                    "gated",
                    gated_mailbox_worker(gate.clone(), worker_ref.clone(), generation.clone()),
                    RestartType::Permanent,
                )
                .with_mailbox(MailboxConfig::bounded(1)),
            )
            .start();

        let actor = wait_for_worker_ref(&worker_ref);
        let _ = actor.request(PanicNow);
        while generation.load(Ordering::SeqCst) < 2 {
            thread::sleep(Duration::from_millis(10));
        }

        let actor = wait_for_worker_ref(&worker_ref);
        actor.send(GatedInc).unwrap();
        thread::sleep(Duration::from_millis(20));
        actor.send(MbPing).unwrap();
        assert!(matches!(actor.send(MbPing), Err(ActorError::MailboxFull)));

        {
            let (lock, cvar) = &*gate;
            *lock.lock().unwrap() = true;
            cvar.notify_all();
        }

        sup.child_handle().stop();
        sup.join();
    }
}
