# Spawned concurrency

Actor framework for Rust, inspired by Erlang/OTP. Define protocols as traits, implement handlers on actors, and call methods directly on actor references.

This crate is part of [spawned](https://github.com/lambdaclass/spawned). See the [workspace README](https://github.com/lambdaclass/spawned/blob/main/README.md) for a full overview, the [API Guide](https://github.com/lambdaclass/spawned/blob/main/docs/API-GUIDE.md) for reference documentation, and the [Supervision Guide](https://github.com/lambdaclass/spawned/blob/main/docs/SUPERVISION.md) for fault-tolerance patterns.

## Quick Example

```rust
use spawned_concurrency::{protocol, Response};
use spawned_concurrency::tasks::{Actor, ActorStart as _, Context, Handler};
use spawned_rt::tasks as rt;

#[protocol]
pub trait CounterProtocol: Send + Sync {
    fn increment(&self);
    fn get(&self) -> Response<u64>;
}

pub struct MyCounter { count: u64 }

impl Actor for MyCounter {}

impl Handler<counter_protocol::Increment> for MyCounter {
    async fn handle(&mut self, _msg: counter_protocol::Increment, _ctx: &Context<Self>) {
        self.count += 1;
    }
}

impl Handler<counter_protocol::Get> for MyCounter {
    async fn handle(&mut self, _msg: counter_protocol::Get, _ctx: &Context<Self>) -> u64 {
        self.count
    }
}

fn main() {
    rt::run(async {
        let counter = MyCounter { count: 0 }.start();
        counter.increment();
        let value = counter.get().await.unwrap();
        assert_eq!(value, 1);
    })
}
```

## Supervision

Built-in Erlang-style supervisors for static trees and runtime child pools:

```ignore
use spawned_concurrency::tasks::{ChildSpec, Supervisor};
use spawned_concurrency::{RestartType, SupervisorStrategy};

let sup = Supervisor::builder()
    .strategy(SupervisorStrategy::OneForOne)
    .child(ChildSpec::worker("worker", || MyWorker::new(), RestartType::Permanent))
    .start();
```

See the [Supervision Guide](https://github.com/lambdaclass/spawned/blob/main/docs/SUPERVISION.md) for restart strategies, shutdown escalation, meltdown, and when to use `DynamicSupervisor`.

## Process Groups

Named actor sets for broadcast and dispatch. Use `tasks::pg` or `threads::pg` with the same API:

```ignore
use spawned_concurrency::tasks::pg;

pg::join("pool", &ctx.actor_ref());
for worker in pg::members::<Worker>("pool") {
    worker.send(Ping)?;
}
```

See the [API Guide — Process Groups](https://github.com/lambdaclass/spawned/blob/main/docs/API-GUIDE.md#process-groups) and [`pg_workers`](../examples/pg_workers).

## Two Execution Modes

- **tasks**: async/await with tokio. Use `spawned_concurrency::tasks`.
- **threads**: blocking, no async runtime. Use `spawned_concurrency::threads`.

Both provide the same `Actor`, `Handler<M>`, `ActorRef<A>`, and `Context<A>` types.
