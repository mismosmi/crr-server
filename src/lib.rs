#![feature(error_generic_member_access)]
#![feature(provide_any)]

pub mod app_state;
pub mod auth;
mod database;
pub(crate) mod error;
pub(crate) mod mail;
mod serde_base64;

use app_state::AppState;
use axum::Router;

pub fn router() -> Router<AppState> {
    Router::<AppState>::new()
        .nest("/auth", auth::router())
        .nest("/db", database::router())
}
