//! Integration tests for DynamicSupervisor — runtime child pools.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use spawned_concurrency::{registry, DynamicSupervisorError, RestartIntensity, RestartType};

mod tasks {
    use super::*;
    use spawned_concurrency::tasks::{
        dynamic_supervisor::ChildSpec, Actor, Context, DynamicSupervisor, DynamicSupervisorApi,
    };
    use spawned_rt::tasks as rt;

    struct CountingWorker {
        starts: Arc<AtomicUsize>,
    }

    impl Actor for CountingWorker {
        async fn started(&mut self, _ctx: &Context<Self>) {
            self.starts.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct FlakyWorker {
        starts: Arc<AtomicUsize>,
    }

    impl Actor for FlakyWorker {
        async fn started(&mut self, _ctx: &Context<Self>) {
            let n = self.starts.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                panic!("first start dies");
            }
        }
    }

    fn run<F: std::future::Future>(f: F) {
        rt::Runtime::new().unwrap().block_on(f);
    }

    #[test]
    fn dynamic_pool_starts_multiple_children() {
        run(async {
            let starts = Arc::new(AtomicUsize::new(0));
            let sup = DynamicSupervisor::builder().start();

            for _ in 0..3 {
                sup.start_child(
                    ChildSpec::worker(
                        "worker",
                        {
                            let starts = starts.clone();
                            move || CountingWorker {
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

            assert_eq!(sup.count_children().await.unwrap(), 3);
            assert_eq!(starts.load(Ordering::SeqCst), 3);

            sup.child_handle().stop();
            sup.join().await;
        });
    }

    #[test]
    fn crash_restarts_single_dynamic_child() {
        run(async {
            let starts = Arc::new(AtomicUsize::new(0));
            let sup = DynamicSupervisor::builder()
                .intensity(RestartIntensity {
                    max_restarts: 5,
                    within: Duration::from_secs(5),
                })
                .start();

            sup.start_child(
                ChildSpec::worker(
                    "flaky",
                    {
                        let starts = starts.clone();
                        move || FlakyWorker {
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

            for _ in 0..50 {
                if starts.load(Ordering::SeqCst) >= 2 {
                    break;
                }
                rt::sleep(Duration::from_millis(20)).await;
            }
            assert_eq!(starts.load(Ordering::SeqCst), 2);
            assert_eq!(sup.count_children().await.unwrap(), 1);

            sup.child_handle().stop();
            sup.join().await;
        });
    }

    #[test]
    fn max_children_rejects_extra_starts() {
        run(async {
            let sup = DynamicSupervisor::builder().max_children(2).start();

            sup.start_child(
                ChildSpec::worker(
                    "w",
                    || CountingWorker {
                        starts: Arc::new(AtomicUsize::new(0)),
                    },
                    RestartType::Permanent,
                ),
                None,
            )
            .await
            .unwrap()
            .unwrap();
            sup.start_child(
                ChildSpec::worker(
                    "w",
                    || CountingWorker {
                        starts: Arc::new(AtomicUsize::new(0)),
                    },
                    RestartType::Permanent,
                ),
                None,
            )
            .await
            .unwrap()
            .unwrap();

            let err = sup
                .start_child(
                    ChildSpec::worker(
                        "w",
                        || CountingWorker {
                            starts: Arc::new(AtomicUsize::new(0)),
                        },
                        RestartType::Permanent,
                    ),
                    None,
                )
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(err, DynamicSupervisorError::MaxChildrenExceeded);

            sup.child_handle().stop();
            sup.join().await;
        });
    }

    #[test]
    fn registered_child_is_lookup_via_registry() {
        run(async {
            let sup = DynamicSupervisor::builder().start();

            sup.start_child(
                ChildSpec::worker(
                    "w",
                    || CountingWorker {
                        starts: Arc::new(AtomicUsize::new(0)),
                    },
                    RestartType::Permanent,
                ),
                Some("pool-worker".into()),
            )
            .await
            .unwrap()
            .unwrap();

            assert!(registry::whereis::<spawned_concurrency::ChildHandle>("pool-worker").is_some());

            let children = sup.which_children().await.unwrap();
            sup.terminate_child(children[0].actor_id)
                .await
                .unwrap()
                .unwrap();
            assert!(registry::whereis::<spawned_concurrency::ChildHandle>("pool-worker").is_none());

            sup.child_handle().stop();
            sup.join().await;
        });
    }
}

mod threads {
    use super::*;
    use spawned_concurrency::threads::{
        dynamic_supervisor::ChildSpec, Actor, Context, DynamicSupervisor, DynamicSupervisorApi,
    };
    use std::thread;

    struct CountingWorker {
        starts: Arc<AtomicUsize>,
    }

    impl Actor for CountingWorker {
        fn started(&mut self, _ctx: &Context<Self>) {
            self.starts.fetch_add(1, Ordering::SeqCst);
        }
    }

    struct FlakyWorker {
        starts: Arc<AtomicUsize>,
    }

    impl Actor for FlakyWorker {
        fn started(&mut self, _ctx: &Context<Self>) {
            let n = self.starts.fetch_add(1, Ordering::SeqCst);
            if n == 0 {
                panic!("first start dies");
            }
        }
    }

    #[test]
    fn dynamic_pool_starts_multiple_children() {
        let starts = Arc::new(AtomicUsize::new(0));
        let sup = DynamicSupervisor::builder().start();

        for _ in 0..3 {
            sup.start_child(
                ChildSpec::worker(
                    "worker",
                    {
                        let starts = starts.clone();
                        move || CountingWorker {
                            starts: starts.clone(),
                        }
                    },
                    RestartType::Permanent,
                ),
                None,
            )
            .unwrap()
            .unwrap();
        }

        assert_eq!(sup.count_children().unwrap(), 3);
        assert_eq!(starts.load(Ordering::SeqCst), 3);

        sup.child_handle().stop();
        sup.join();
    }

    #[test]
    fn crash_restarts_single_dynamic_child() {
        let starts = Arc::new(AtomicUsize::new(0));
        let sup = DynamicSupervisor::builder()
            .intensity(RestartIntensity {
                max_restarts: 5,
                within: Duration::from_secs(5),
            })
            .start();

        sup.start_child(
            ChildSpec::worker(
                "flaky",
                {
                    let starts = starts.clone();
                    move || FlakyWorker {
                        starts: starts.clone(),
                    }
                },
                RestartType::Permanent,
            ),
            None,
        )
        .unwrap()
        .unwrap();

        for _ in 0..50 {
            if starts.load(Ordering::SeqCst) >= 2 {
                break;
            }
            thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(starts.load(Ordering::SeqCst), 2);
        assert_eq!(sup.count_children().unwrap(), 1);

        sup.child_handle().stop();
        sup.join();
    }
}
