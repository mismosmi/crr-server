use std::time::Duration;

use axum::{Router, Server};
use crr_server::{app_state::AppState, auth::AuthDatabase, router};
use rusqlite::params;
use tokio::process::Command;
use tracing_test::traced_test;

async fn setup_and_install() {
    let out = Command::new("pnpm")
        .current_dir(std::fs::canonicalize("drizzle").unwrap())
        .arg("install")
        .output()
        .await
        .unwrap();

    assert!(out.status.success());
}

fn prepare_app(token: &str) -> Router<()> {
    let state = AppState::test_state();

    let auth = AuthDatabase::open(state.env().clone()).unwrap();

    auth.prepare("INSERT INTO users (email) VALUES (?)")
        .unwrap()
        .insert(["test@michelsmola.de"])
        .unwrap();

    let user_id = auth.last_insert_rowid();

    auth.prepare(
        "INSERT INTO tokens (user_id, token, expires) VALUES (?, ?, DATE('now', '+1 day'))",
    )
    .unwrap()
    .insert(params![user_id, token])
    .unwrap();

    router().with_state(state)
}

async fn run_tests(url: &str, token: &str) {
    let output = Command::new("pnpm")
        .current_dir(std::fs::canonicalize("drizzle").unwrap())
        .env("CRR_SERVER_URL", url)
        .env("CRR_SERVER_TOKEN", token)
        .arg("test")
        .output()
        .await
        .unwrap();

    tracing::info!("{}", String::from_utf8(output.stdout).unwrap());
    tracing::error!("{}", String::from_utf8(output.stderr).unwrap());
    //println!("{}", String::from_utf8(out.stderr).unwrap());

    assert!(output.status.success());
}

#[traced_test]
#[tokio::test]
async fn run_migrations() {
    setup_and_install().await;

    let token = nanoid::nanoid!();

    let server = Server::bind(&"0.0.0.0:6840".parse().unwrap())
        .serve(prepare_app(&token).into_make_service());

    let url = server.local_addr();

    server
        .with_graceful_shutdown(async {
            tokio::time::sleep(Duration::from_secs(5)).await;
            run_tests(&format!("http://{}", url.to_string()), &token).await;
        })
        .await
        .unwrap();
}
