use axum::{
    routing::{get, post},
    Router,
};

use crate::app_state::AppState;

use self::{otp::post_otp, signed_url::get_signed_url, token::post_token};

mod database;
mod otp;
mod permissions;
mod signed_url;
mod token;

pub use database::AuthDatabase;
pub(crate) use permissions::{AllowedTables, DatabasePermissions};
pub(crate) use token::Token;

#[cfg(test)]
pub(crate) use permissions::PartialPermissions;

pub(crate) const COOKIE_NAME: &'static str = "CRR_TOKEN";

pub(crate) fn router() -> Router<AppState> {
    Router::new()
        .route("/otp", post(post_otp))
        .route("/token", post(post_token))
        .route("/signed-url", get(get_signed_url))
}
