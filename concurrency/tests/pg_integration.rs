//! Integration tests for process groups — exercises the public crate API end-to-end.

use spawned_concurrency::pg;
use spawned_concurrency::{message::Message, ChildHandle, PgError};
use std::sync::atomic::{AtomicU64, Ordering};

static GROUP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_group(prefix: &str) -> String {
    format!("{prefix}_{}", GROUP_COUNTER.fetch_add(1, Ordering::Relaxed))
}

#[derive(Clone, Copy)]
struct Tick;

impl Message for Tick {
    type Result = u32;
}

mod tasks {
    use super::*;
    use spawned_concurrency::tasks::{pg as tasks_pg, Actor, ActorStart, Context, Handler};
    use spawned_rt::tasks as rt;

    struct Counter {
        value: u32,
        group: String,
    }

    impl Actor for Counter {
        async fn started(&mut self, ctx: &Context<Self>) {
            tasks_pg::join(&self.group, &ctx.actor_ref());
        }
    }

    impl Handler<Tick> for Counter {
        async fn handle(&mut self, _msg: Tick, _ctx: &Context<Self>) -> u32 {
            self.value += 1;
            self.value
        }
    }

    struct JoinOnStart;

    impl Message for JoinOnStart {
        type Result = ();
    }

    struct Coordinator {
        group: String,
    }

    impl Actor for Coordinator {}

    impl Handler<JoinOnStart> for Coordinator {
        async fn handle(&mut self, _msg: JoinOnStart, ctx: &Context<Self>) {
            tasks_pg::join(&self.group, &ctx.actor_ref());
        }
    }

    fn block_on<F: std::future::Future>(f: F) -> F::Output {
        rt::Runtime::new().unwrap().block_on(f)
    }

    #[test]
    fn pg_join_members_and_broadcast() {
        block_on(async {
            let group = unique_group("tasks_pg_join");
            let _c1 = Counter {
                value: 0,
                group: group.clone(),
            }
            .start();
            let _c2 = Counter {
                value: 10,
                group: group.clone(),
            }
            .start();

            rt::sleep(std::time::Duration::from_millis(20)).await;

            let members = tasks_pg::members::<Counter>(&group);
            assert_eq!(members.len(), 2);

            let mut sum = 0u32;
            for member in &members {
                sum += member.request(Tick).await.unwrap();
            }
            assert_eq!(sum, 12); // 1 + 11
        });
    }

    #[test]
    fn pg_auto_leave_on_actor_exit() {
        block_on(async {
            let group = unique_group("tasks_pg_auto_leave");
            let counter = Counter {
                value: 0,
                group: group.clone(),
            }
            .start();
            rt::sleep(std::time::Duration::from_millis(20)).await;
            assert_eq!(tasks_pg::members::<Counter>(&group).len(), 1);

            let id = counter.id();
            counter.child_handle().stop();
            counter.join().await;

            rt::sleep(std::time::Duration::from_millis(20)).await;
            assert_eq!(tasks_pg::members::<Counter>(&group).len(), 0);
            assert!(pg::get_members(&group).iter().all(|h| h.id() != id));
        });
    }

    #[test]
    fn pg_leave_refcount_and_errors() {
        block_on(async {
            let group = unique_group("tasks_pg_refcount");
            let actor = Coordinator {
                group: group.clone(),
            }
            .start();
            actor.request(JoinOnStart).await.unwrap();
            actor.request(JoinOnStart).await.unwrap();

            let id = actor.id();
            tasks_pg::leave(&group, id).unwrap();
            assert_eq!(tasks_pg::members::<Coordinator>(&group).len(), 1);

            tasks_pg::leave(&group, id).unwrap();
            assert!(tasks_pg::members::<Coordinator>(&group).is_empty());

            let err = tasks_pg::leave(&group, id).unwrap_err();
            assert_eq!(
                err,
                PgError::NotJoined(id, group, spawned_concurrency::DEFAULT_SCOPE.to_string())
            );
        });
    }

    #[test]
    fn pg_child_handle_membership() {
        block_on(async {
            let group = unique_group("tasks_pg_handle");
            let counter = Counter {
                value: 0,
                group: group.clone(),
            }
            .start();
            rt::sleep(std::time::Duration::from_millis(20)).await;

            let handle: ChildHandle = counter.child_handle();
            let members: Vec<ChildHandle> = pg::get_members(&group);
            assert!(members.iter().any(|h| h.id() == handle.id()));
        });
    }

    #[test]
    fn pg_which_groups() {
        block_on(async {
            let unique = unique_group("tasks_pg_which");
            let counter = Counter {
                value: 0,
                group: unique.clone(),
            }
            .start();
            pg::join(&unique, counter.child_handle());
            rt::sleep(std::time::Duration::from_millis(10)).await;
            assert!(pg::which_groups().contains(&unique));
        });
    }

    #[test]
    fn pg_scoped_membership_isolated() {
        block_on(async {
            let group = unique_group("tasks_pg_scope");
            let scope_a = unique_group("scope_a");
            let scope_b = unique_group("scope_b");

            let _c1 = Counter {
                value: 0,
                group: group.clone(),
            }
            .start();
            rt::sleep(std::time::Duration::from_millis(20)).await;

            let member = tasks_pg::members::<Counter>(&group)[0].clone();
            tasks_pg::leave(&group, member.id()).unwrap();
            tasks_pg::join_scoped(&scope_a, &group, &member);

            assert!(tasks_pg::members::<Counter>(&group).is_empty());
            assert_eq!(tasks_pg::members_scoped::<Counter>(&scope_a, &group).len(), 1);
            assert!(tasks_pg::members_scoped::<Counter>(&scope_b, &group).is_empty());
            assert!(pg::which_scopes().contains(&scope_a));
        });
    }

    #[test]
    fn pg_cast_and_call() {
        block_on(async {
            let group = unique_group("tasks_pg_cast");
            let _c1 = Counter {
                value: 0,
                group: group.clone(),
            }
            .start();
            let _c2 = Counter {
                value: 10,
                group: group.clone(),
            }
            .start();
            rt::sleep(std::time::Duration::from_millis(20)).await;

            let cast = tasks_pg::cast::<Counter, _>(&group, Tick);
            assert_eq!(cast.delivered, 2);
            assert!(cast.failed.is_empty());

            let call = tasks_pg::call::<Counter, _>(&group, Tick).await;
            assert_eq!(call.ok.len(), 2);
            assert!(call.failed.is_empty());
            let sum: u32 = call.ok.iter().map(|(_, v)| *v).sum();
            assert_eq!(sum, 14); // cast then call: (1+11) + (1+1) wait - after cast: 1,11. after call: 2,12. sum=14
        });
    }

    #[test]
    fn child_spec_with_pg_group_auto_joins() {
        block_on(async {
            use spawned_concurrency::tasks::{
                dynamic_supervisor::ChildSpec, DynamicSupervisor, DynamicSupervisorApi,
            };

            struct PgWorker;
            impl Actor for PgWorker {}
            impl Handler<Tick> for PgWorker {
                async fn handle(&mut self, _msg: Tick, _ctx: &Context<Self>) -> u32 {
                    1
                }
            }

            let group = unique_group("tasks_pg_auto_join");
            let sup = DynamicSupervisor::builder().start();

            sup.start_child(
                ChildSpec::worker("worker", || PgWorker, spawned_concurrency::RestartType::Permanent)
                    .with_pg_group(&group),
                None,
            )
            .await
            .unwrap()
            .unwrap();

            rt::sleep(std::time::Duration::from_millis(20)).await;
            assert_eq!(tasks_pg::members::<PgWorker>(&group).len(), 1);

            sup.child_handle().stop();
            sup.join().await;
        });
    }
}

mod threads {
    use super::*;
    use spawned_concurrency::threads::{pg as threads_pg, Actor, ActorStart, Context, Handler};
    use std::thread;
    use std::time::Duration;

    struct Counter {
        value: u32,
        group: String,
    }

    impl Actor for Counter {
        fn started(&mut self, ctx: &Context<Self>) {
            threads_pg::join(&self.group, &ctx.actor_ref());
        }
    }

    impl Handler<Tick> for Counter {
        fn handle(&mut self, _msg: Tick, _ctx: &Context<Self>) -> u32 {
            self.value += 1;
            self.value
        }
    }

    struct JoinOnStart;

    impl Message for JoinOnStart {
        type Result = ();
    }

    struct Coordinator {
        group: String,
    }

    impl Actor for Coordinator {}

    impl Handler<JoinOnStart> for Coordinator {
        fn handle(&mut self, _msg: JoinOnStart, ctx: &Context<Self>) {
            threads_pg::join(&self.group, &ctx.actor_ref());
        }
    }

    #[test]
    fn pg_join_members_and_broadcast() {
        let group = unique_group("threads_pg_join");
        let _c1 = Counter {
            value: 0,
            group: group.clone(),
        }
        .start();
        let _c2 = Counter {
            value: 10,
            group: group.clone(),
        }
        .start();

        thread::sleep(Duration::from_millis(20));

        let members = threads_pg::members::<Counter>(&group);
        assert_eq!(members.len(), 2);

        let mut sum = 0u32;
        for member in &members {
            sum += member.request(Tick).unwrap();
        }
        assert_eq!(sum, 12); // 1 + 11
    }

    #[test]
    fn pg_auto_leave_on_actor_exit() {
        let group = unique_group("threads_pg_auto_leave");
        let counter = Counter {
            value: 0,
            group: group.clone(),
        }
        .start();
        thread::sleep(Duration::from_millis(20));
        assert_eq!(threads_pg::members::<Counter>(&group).len(), 1);

        let id = counter.id();
        counter.child_handle().stop();
        counter.join();

        thread::sleep(Duration::from_millis(20));
        assert_eq!(threads_pg::members::<Counter>(&group).len(), 0);
        assert!(pg::get_members(&group).iter().all(|h| h.id() != id));
    }

    #[test]
    fn pg_leave_refcount_and_errors() {
        let group = unique_group("threads_pg_refcount");
        let actor = Coordinator {
            group: group.clone(),
        }
        .start();
        actor.request(JoinOnStart).unwrap();
        actor.request(JoinOnStart).unwrap();

        let id = actor.id();
        threads_pg::leave(&group, id).unwrap();
        assert_eq!(threads_pg::members::<Coordinator>(&group).len(), 1);

        threads_pg::leave(&group, id).unwrap();
        assert!(threads_pg::members::<Coordinator>(&group).is_empty());

        let err = threads_pg::leave(&group, id).unwrap_err();
        assert_eq!(
            err,
            PgError::NotJoined(id, group, spawned_concurrency::DEFAULT_SCOPE.to_string())
        );
    }

    #[test]
    fn pg_child_handle_membership() {
        let group = unique_group("threads_pg_handle");
        let counter = Counter {
            value: 0,
            group: group.clone(),
        }
        .start();
        thread::sleep(Duration::from_millis(20));

        let handle: ChildHandle = counter.child_handle();
        let members: Vec<ChildHandle> = pg::get_members(&group);
        assert!(members.iter().any(|h| h.id() == handle.id()));
    }

    #[test]
    fn pg_which_groups() {
        let unique = unique_group("threads_pg_which");
        let counter = Counter {
            value: 0,
            group: unique.clone(),
        }
        .start();
        pg::join(&unique, counter.child_handle());
        thread::sleep(Duration::from_millis(10));
        assert!(pg::which_groups().contains(&unique));
    }

    #[test]
    fn pg_cast_and_call() {
        let group = unique_group("threads_pg_cast");
        let _c1 = Counter {
            value: 0,
            group: group.clone(),
        }
        .start();
        let _c2 = Counter {
            value: 10,
            group: group.clone(),
        }
        .start();
        thread::sleep(Duration::from_millis(20));

        let cast = threads_pg::cast::<Counter, _>(&group, Tick);
        assert_eq!(cast.delivered, 2);
        assert!(cast.failed.is_empty());

        let call = threads_pg::call::<Counter, _>(&group, Tick);
        assert_eq!(call.ok.len(), 2);
        assert!(call.failed.is_empty());
        let sum: u32 = call.ok.iter().map(|(_, v)| *v).sum();
        assert_eq!(sum, 14);
    }

    #[test]
    fn child_spec_with_pg_group_auto_joins() {
        use spawned_concurrency::threads::{
            dynamic_supervisor::ChildSpec, DynamicSupervisor, DynamicSupervisorApi,
        };

        struct PgWorker;
        impl Actor for PgWorker {}
        impl Handler<Tick> for PgWorker {
            fn handle(&mut self, _msg: Tick, _ctx: &Context<Self>) -> u32 {
                1
            }
        }

        let group = unique_group("threads_pg_auto_join");
        let sup = DynamicSupervisor::builder().start();

        sup.start_child(
            ChildSpec::worker("worker", || PgWorker, spawned_concurrency::RestartType::Permanent)
                .with_pg_group(&group),
            None,
        )
        .unwrap()
        .unwrap();

        thread::sleep(Duration::from_millis(20));
        assert_eq!(threads_pg::members::<PgWorker>(&group).len(), 1);

        sup.child_handle().stop();
        sup.join();
    }
}
