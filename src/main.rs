mod auth;
mod database;
pub(crate) mod error;
pub(crate) mod mail;
mod serde_base64;
#[cfg(test)]
mod tests;

use auth::database::AuthDatabase;
use axum::{Router, Server};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    dotenv::dotenv().expect("Failed to read environment");

    let auth = AuthDatabase::open().expect("Failed to open Auth Database");

    auth.apply_migrations()
        .expect("Failed to apply Auth Migrations");

    let app = Router::new()
        .nest("/auth", auth::router())
        .nest("/db", database::router());

    tracing::info!("Starting server...");
    Server::bind(
        &"0.0.0.0:3000"
            .parse()
            .expect("Failed to parse bind address"),
    )
    .serve(app.into_make_service())
    .await
    .expect("Failed to start server");
}
