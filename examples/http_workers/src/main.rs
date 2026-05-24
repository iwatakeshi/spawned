use axum::{
    Router,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Json},
    routing::{get, post},
};
use serde::Serialize;
use spawned_concurrency::error::ActorError;
use spawned_concurrency::message::Message;
use spawned_concurrency::tasks::{
    dynamic_supervisor::ChildSpec, pg, Actor, Context, DynamicSupervisor, DynamicSupervisorApi,
    Handler,
};
use spawned_concurrency::{MailboxConfig, Node, RestartType};
use spawned_rt::tasks as rt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

const GROUP: &str = "http_workers";
const WORKER_MAILBOX: usize = 4;
const WORK_MS: u64 = 200;

#[derive(Clone)]
struct AppState {
    next_worker: Arc<AtomicUsize>,
}

#[derive(Serialize)]
struct WorkerStat {
    id: String,
    depth: usize,
    capacity: Option<usize>,
}

#[derive(Serialize)]
struct StatsResponse {
    workers: Vec<WorkerStat>,
}

struct ProcessRequest;
impl Message for ProcessRequest {
    type Result = ();
}

struct HttpWorker {
    name: String,
}

impl Actor for HttpWorker {
    async fn started(&mut self, ctx: &Context<Self>) {
        pg::join(GROUP, &ctx.actor_ref());
        tracing::info!("[{}] ready (mailbox cap {})", self.name, WORKER_MAILBOX);
    }

    async fn stopped(&mut self, ctx: &Context<Self>) {
        let _ = pg::leave(GROUP, ctx.id());
    }
}

impl Handler<ProcessRequest> for HttpWorker {
    async fn handle(&mut self, _msg: ProcessRequest, _ctx: &Context<Self>) {
        rt::sleep(Duration::from_millis(WORK_MS)).await;
    }
}

async fn post_work(State(state): State<AppState>) -> impl IntoResponse {
    let workers = pg::members::<HttpWorker>(GROUP);
    if workers.is_empty() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            "no workers available".to_string(),
        )
            .into_response();
    }

    let idx = state.next_worker.fetch_add(1, Ordering::Relaxed) % workers.len();
    match workers[idx].send(ProcessRequest) {
        Ok(()) => (StatusCode::OK, "accepted".to_string()).into_response(),
        Err(ActorError::MailboxFull) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "worker mailbox full — load shed".to_string(),
        )
            .into_response(),
        Err(ActorError::ActorStopped) => (
            StatusCode::SERVICE_UNAVAILABLE,
            "worker stopped".to_string(),
        )
            .into_response(),
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("dispatch error: {err}"),
        )
            .into_response(),
    }
}

async fn get_stats() -> Json<StatsResponse> {
    let workers = pg::members::<HttpWorker>(GROUP)
        .into_iter()
        .map(|worker| WorkerStat {
            id: worker.id().to_string(),
            depth: worker.mailbox_depth(),
            capacity: worker.mailbox_capacity(),
        })
        .collect();
    Json(StatsResponse { workers })
}

fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(true)
        .init();

    rt::run(async {
        let sup = DynamicSupervisor::builder().max_children(8).start();

        for i in 0..3 {
            sup.start_child(
                ChildSpec::worker(
                    "http_worker",
                    move || HttpWorker {
                        name: format!("worker-{i}"),
                    },
                    RestartType::Permanent,
                )
                .with_mailbox(MailboxConfig::bounded(WORKER_MAILBOX)),
                None,
            )
            .await
            .unwrap()
            .unwrap();
        }

        rt::sleep(Duration::from_millis(100)).await;

        let app = Router::new()
            .route("/work", post(post_work))
            .route("/stats", get(get_stats))
            .with_state(AppState {
                next_worker: Arc::new(AtomicUsize::new(0)),
            });

        let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
            .await
            .expect("bind port 3000");
        println!("=== HTTP Workers Demo ===");
        println!("Workers: 3 x bounded mailbox (cap {WORKER_MAILBOX}), ~{WORK_MS}ms handler");
        println!();
        println!("  curl -X POST http://127.0.0.1:3000/work");
        println!("  curl http://127.0.0.1:3000/stats");
        println!();
        println!("Burst POST /work to observe 503 when mailboxes fill.");
        println!("Press Ctrl+C or send SIGTERM to stop.\n");

        let node = Node::builder()
            .shutdown_on_signal(&[sup.child_handle()])
            .build()
            .expect("start node");
        let _node = node;

        rt::spawn(async move {
            if let Err(err) = axum::serve(listener, app).await {
                tracing::error!("HTTP server error: {err}");
            }
        });

        sup.join().await;
        println!("\nShutdown complete.");
    });
}
