mod app_state;
mod auth;
mod database;
pub(crate) mod error;
pub(crate) mod mail;
mod serde_base64;

pub(crate) use app_state::AppState;
use auth::AuthDatabase;
use axum::{Router, Server};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    dotenv::dotenv().expect("Failed to read environment");

    let state = AppState::init();

    let auth = AuthDatabase::open(state.env().clone()).expect("Failed to open Auth Database");

    auth.apply_migrations()
        .expect("Failed to apply Auth Migrations");

    let app = Router::<AppState>::new()
        .nest("/auth", auth::router())
        .nest("/db", database::router())
        .with_state(state);

    tracing::info!("Starting server...");
    Server::bind(
        &"0.0.0.0:6839"
            .parse()
            .expect("Failed to parse bind address"),
    )
    .serve(app.into_make_service())
    .await
    .expect("Failed to start server");
}
