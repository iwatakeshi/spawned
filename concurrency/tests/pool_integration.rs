//! Integration tests for actor pools (DynamicSupervisor + pg dispatch).

use spawned_concurrency::message::Message;
use spawned_concurrency::pool::{PoolDispatcher, PoolError, PoolStrategy};
use std::sync::atomic::{AtomicU64, Ordering};

static GROUP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_group(prefix: &str) -> String {
    format!("{prefix}_{}", GROUP_COUNTER.fetch_add(1, Ordering::Relaxed))
}

#[derive(Clone, Copy)]
struct Work;

impl Message for Work {
    type Result = u32;
}

mod tasks {
    use super::*;
    use spawned_concurrency::tasks::{pg, pool, Actor, ActorPool, ChildSpec, Context, Handler};
    use spawned_concurrency::{MailboxConfig, RestartType};
    use spawned_rt::tasks as rt;

    struct Worker {
        group: String,
        value: u32,
    }

    impl Actor for Worker {
        async fn started(&mut self, ctx: &Context<Self>) {
            pg::join(&self.group, &ctx.actor_ref());
        }
    }

    impl Handler<Work> for Worker {
        async fn handle(&mut self, _msg: Work, _ctx: &Context<Self>) -> u32 {
            self.value += 1;
            self.value
        }
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        rt::Runtime::new().unwrap().block_on(f)
    }

    #[test]
    fn actor_pool_round_robin_dispatch() {
        block_on(async {
            let group = unique_group("tasks_pool_rr");
            let pool = ActorPool::builder(&group)
                .start(3, |i| {
                    let g = group.clone();
                    ChildSpec::worker(
                        "worker",
                        move || Worker {
                            group: g.clone(),
                            value: i as u32 * 10,
                        },
                        RestartType::Permanent,
                    )
                    .with_mailbox(MailboxConfig::bounded(8))
                })
                .await;

            rt::sleep(std::time::Duration::from_millis(30)).await;
            for _ in 0..50 {
                if pg::members::<Worker>(&group).len() == 3 {
                    break;
                }
                rt::sleep(std::time::Duration::from_millis(10)).await;
            }
            assert_eq!(pg::members::<Worker>(&group).len(), 3);

            let d = PoolDispatcher::new(&group, PoolStrategy::RoundRobin);
            let r1 = pool::call_one::<Worker, _>(&d, Work).await.unwrap();
            let r2 = pool::call_one::<Worker, _>(&d, Work).await.unwrap();
            let r3 = pool::call_one::<Worker, _>(&d, Work).await.unwrap();
            let mut results = [r1, r2, r3];
            results.sort();
            assert_eq!(results, [1, 11, 21]);

            pool.dispatch::<Worker, _>(Work).unwrap();
        });
    }

    #[test]
    fn empty_pool_returns_no_members() {
        block_on(async {
            let group = unique_group("tasks_pool_empty");
            let d = PoolDispatcher::new(&group, PoolStrategy::RoundRobin);
            assert_eq!(
                pool::dispatch::<Worker, _>(&d, Work).unwrap_err(),
                PoolError::NoMembers
            );
        });
    }
}

mod threads {
    use super::*;
    use spawned_concurrency::threads::{pg, pool, Actor, ActorPool, ChildSpec, Context, Handler};
    use spawned_concurrency::{MailboxConfig, RestartType};
    use std::time::Duration;

    struct Worker {
        group: String,
        value: u32,
    }

    impl Actor for Worker {
        fn started(&mut self, ctx: &Context<Self>) {
            pg::join(&self.group, &ctx.actor_ref());
        }
    }

    impl Handler<Work> for Worker {
        fn handle(&mut self, _msg: Work, _ctx: &Context<Self>) -> u32 {
            self.value += 1;
            self.value
        }
    }

    #[test]
    fn actor_pool_call_one() {
        let group = unique_group("threads_pool");
        let pool = ActorPool::builder(&group).start(2, |i| {
            let g = group.clone();
            ChildSpec::worker(
                "worker",
                move || Worker {
                    group: g.clone(),
                    value: i as u32,
                },
                RestartType::Permanent,
            )
            .with_mailbox(MailboxConfig::bounded(4))
        });

        std::thread::sleep(Duration::from_millis(30));

        let d = PoolDispatcher::new(&group, PoolStrategy::RoundRobin);
        let r1 = pool::call_one::<Worker, _>(&d, Work).unwrap();
        let r2 = pool::call_one::<Worker, _>(&d, Work).unwrap();
        let mut results = [r1, r2];
        results.sort();
        assert_eq!(results, [1, 2]);

        pool.dispatch::<Worker, _>(Work).unwrap();
    }
}
