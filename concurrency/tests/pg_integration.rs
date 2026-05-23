use spawned_concurrency::pg;
use spawned_concurrency::tasks::{pg as tasks_pg, Actor, ActorStart, Context, Handler};
use spawned_concurrency::{message::Message, ChildHandle, PgError};
use spawned_rt::tasks as rt;
use std::sync::atomic::{AtomicU64, Ordering};

static GROUP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn unique_group(prefix: &str) -> String {
    format!("{prefix}_{}", GROUP_COUNTER.fetch_add(1, Ordering::Relaxed))
}

struct Tick;

impl Message for Tick {
    type Result = u32;
}

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
        let group = unique_group("pg_join");
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
        let group = unique_group("pg_auto_leave");
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
        let group = unique_group("pg_refcount");
        let actor = Coordinator {
            group: group.clone(),
        }
        .start();
        actor.request(JoinOnStart).await.unwrap();
        actor.request(JoinOnStart).await.unwrap();

        let id = actor.id();
        tasks_pg::leave::<Coordinator>(&group, id).unwrap();
        assert_eq!(tasks_pg::members::<Coordinator>(&group).len(), 1);

        tasks_pg::leave::<Coordinator>(&group, id).unwrap();
        assert!(tasks_pg::members::<Coordinator>(&group).is_empty());

        let err = tasks_pg::leave::<Coordinator>(&group, id).unwrap_err();
        assert_eq!(err, PgError::NotJoined(id, group));
    });
}

#[test]
fn pg_child_handle_membership() {
    block_on(async {
        let group = unique_group("pg_handle");
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
        let unique = unique_group("pg_which");
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
