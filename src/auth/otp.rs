use std::sync::Arc;

use axum::extract::{Json, State};
use serde::Deserialize;

use crate::{app_state::AppState, error::CRRError};

use super::database::AuthDatabase;

#[derive(Deserialize)]
pub(crate) struct OtpRequestData {
    email: String,
}

pub(crate) async fn post_otp(
    State(state): State<AppState>,
    Json(data): Json<OtpRequestData>,
) -> Result<String, CRRError> {
    let auth = AuthDatabase::open(Arc::clone(state.env()))?;

    let otp = nanoid::nanoid!();

    let mut stmt = auth.prepare(
        "
        INSERT INTO users (email, otp)
        VALUES (:email, :otp)
        ON CONFLICT (email) DO UPDATE SET otp = :otp;
    ",
    )?;

    stmt.insert(rusqlite::named_params! { ":email": data.email, ":otp": otp})?;

    if state.env().disable_validation() {
        return Ok(otp);
    }

    crate::mail::send_email(&data.email, "Your OTP".to_owned(), otp)?;
    Ok("".to_owned())
}
