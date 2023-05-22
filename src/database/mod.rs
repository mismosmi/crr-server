mod changeset;
mod database;
mod migrate;
mod run;
mod value;

use axum::{routing::post, Router};
pub(crate) use database::Database;
pub(crate) use value::Value;

use self::migrate::post_migrate;

pub(crate) fn router() -> Router {
    Router::new().route("/migrate", post(post_migrate))
}
