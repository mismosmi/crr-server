use axum::{routing::post, Router};

use crate::app_state::AppState;

use self::{otp::post_otp, token::post_token};

pub(crate) mod database;
pub(crate) mod otp;
mod permissions;
pub(crate) mod token;
pub(crate) mod user;

pub(crate) use permissions::{AllowedTables, DatabasePermissions};

pub(crate) const COOKIE_NAME: &'static str = "CRR_TOKEN";

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/otp", post(post_otp))
        .route("/token", post(post_token))
}
