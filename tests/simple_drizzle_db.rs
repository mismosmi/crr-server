use std::process::{Command, Stdio};

use axum::Server;
use crr_server::{app_state::AppState, auth::AuthDatabase, router};
use rusqlite::params;
use tokio::task::JoinHandle;

fn setup_and_install() {
    let out = Command::new("pnpm")
        .current_dir(std::fs::canonicalize("drizzle").unwrap())
        .arg("install")
        .output()
        .unwrap();

    assert!(out.status.success());
}

struct ServerHandle {
    join_handle: JoinHandle<()>,
    token: String,
    url: String,
}

impl std::ops::Drop for ServerHandle {
    fn drop(&mut self) {
        self.join_handle.abort()
    }
}

fn start_server() -> ServerHandle {
    let state = AppState::test_state();

    let token = nanoid::nanoid!();

    let auth = AuthDatabase::open(state.env().clone()).unwrap();

    auth.prepare("INSERT INTO users (email) VALUES (?)")
        .unwrap()
        .insert(["test@michelsmola.de"])
        .unwrap();

    let user_id = auth.last_insert_rowid();

    auth.prepare(
        "INSERT INTO tokens (user_id, token, expires) VALUES (?, ?, JULIANDAY('now') + 1)",
    )
    .unwrap()
    .insert(params![user_id, &token])
    .unwrap();

    let app = router().with_state(state);

    let join_handle = tokio::spawn(async {
        Server::bind(&"0.0.0.0:6840".parse().unwrap())
            .serve(app.into_make_service())
            .await
            .unwrap();
    });

    ServerHandle {
        join_handle,
        token,
        url: "http://127.0.0.1:6840".to_string(),
    }
}

fn run_tests(handle: &ServerHandle) {
    let out = Command::new("pnpm")
        .current_dir(std::fs::canonicalize("drizzle").unwrap())
        .env("CRR_SERVER_URL", &handle.url)
        .env("CRR_SERVER_TOKEN", &handle.token)
        .arg("test")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .output()
        .unwrap();

    //println!("{}", String::from_utf8(out.stderr).unwrap());

    assert!(out.status.success());
}

#[tokio::test]
async fn run_migrations() {
    tracing_subscriber::fmt::init();

    setup_and_install();
    let handle = start_server();
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    run_tests(&handle);
}
