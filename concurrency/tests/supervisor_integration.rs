//! Integration tests for supervision — exercises the public crate API end-to-end.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use spawned_concurrency::{
    ChildHandle, ExitReason, RestartIntensity, RestartType, SupervisorStrategy,
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

// ---------------------------------------------------------------------------
// Tasks mode
// ---------------------------------------------------------------------------

mod tasks {
    use super::*;
    use spawned_concurrency::tasks::{Actor, ActorStart, ChildSpec, Context, Supervisor};
    use spawned_rt::tasks as rt;

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
                wait_until_all(&counts, &["alpha", "beta", "gamma"], 2, Duration::from_secs(2)),
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
            assert_eq!(
                handle.wait_exit_async().await,
                ExitReason::Shutdown
            );

            let actor = Idler.start();
            let handle = actor.child_handle();
            handle.kill();
            assert_eq!(handle.wait_exit_async().await, ExitReason::Kill);
        });
    }
}

// ---------------------------------------------------------------------------
// Threads mode
// ---------------------------------------------------------------------------

mod threads {
    use super::*;
    use spawned_concurrency::threads::{Actor, ActorStart, ChildSpec, Context, Supervisor};
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
            wait_until_all(&counts, &["alpha", "beta", "gamma"], 2, Duration::from_secs(2)),
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
}
