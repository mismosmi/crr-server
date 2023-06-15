use axum::Server;
use clap::Parser;
pub(crate) use crr_server::{app_state::AppState, auth::AuthDatabase, router};

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    disable_validation: bool,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    dotenv::dotenv().expect("Failed to read environment");

    let cli = Cli::parse();

    let state = AppState::init(!cli.disable_validation);

    let auth = AuthDatabase::open(state.env().clone()).expect("Failed to open Auth Database");

    auth.apply_migrations()
        .expect("Failed to apply Auth Migrations");

    let app = router().with_state(state);

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
