use axum::{routing::post, Router};

use crate::app_state::AppState;

use self::{otp::post_otp, token::post_token};

mod database;
mod otp;
mod permissions;
mod token;

pub(crate) use database::AuthDatabase;
pub(crate) use permissions::{AllowedTables, DatabasePermissions};

#[cfg(test)]
pub(crate) use permissions::PartialPermissions;

pub(crate) const COOKIE_NAME: &'static str = "CRR_TOKEN";

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/otp", post(post_otp))
        .route("/token", post(post_token))
}
