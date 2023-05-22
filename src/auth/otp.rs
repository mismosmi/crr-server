use axum::extract::Json;
use serde::Deserialize;

use crate::error::CRRError;

use super::database::AuthDatabase;

#[derive(Deserialize)]
pub(crate) struct OtpRequestData {
    email: String,
}

pub(crate) async fn post_otp(Json(data): Json<OtpRequestData>) -> Result<(), CRRError> {
    let auth = AuthDatabase::open()?;

    let otp = nanoid::nanoid!();

    let mut stmt = auth.prepare(
        "
        INSERT INTO users (email, otp)
        VALUES (:email, :otp)
        ON CONFLICT (email) DO UPDATE SET otp = :otp;
    ",
    )?;

    stmt.insert(rusqlite::named_params! { ":email": data.email, ":otp": otp})?;

    crate::mail::send_email(&data.email, "Your OTP".to_owned(), otp)?;

    Ok(())
}
