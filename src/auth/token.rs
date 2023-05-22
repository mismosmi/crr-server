use axum::extract::Json;
use axum_extra::extract::cookie::{Cookie, CookieJar, SameSite};
use rusqlite::named_params;
use serde::Deserialize;
use time::Duration;

use crate::error::CRRError;

use super::database::AuthDatabase;

#[derive(Deserialize)]
pub(crate) struct TokenRequestData {
    otp: Option<String>,
}

pub(crate) async fn post_token(
    mut cookies: CookieJar,
    Json(data): Json<TokenRequestData>,
) -> Result<CookieJar, CRRError> {
    let auth = AuthDatabase::open()?;

    let user_id: i64 = match data.otp.as_ref() {
        Some(otp) => auth
            .prepare("SELECT id FROM users WHERE otp = :otp")?
            .query_row(named_params! { ":otp": otp }, |row| row.get(0))?,

        None => {
            let token = cookies
                .get(super::COOKIE_NAME)
                .ok_or(CRRError::Unauthorized("Token Not Found".to_owned()))?
                .value();

            auth.prepare("SELECT user_id FROM tokens WHERE token = :token AND expires > 'now'")?
                .query_row(named_params! { ":token": token }, |row| row.get(0))?
        }
    };

    {
        let token = nanoid::nanoid!();

        auth.prepare("INSERT INTO tokens (user_id, token, expires) VALUES (:user_id, :token, JULIANDAY('now') + 400)")?
            .insert(named_params! { ":user_id": user_id, ":token": token })?;

        let cookie = Cookie::build(super::COOKIE_NAME, token)
            .http_only(true)
            .max_age(Duration::days(400))
            .same_site(SameSite::Strict)
            .secure(true)
            .path("/")
            .finish();

        cookies = cookies.add(cookie);
    }

    auth.prepare("UPDATE users SET otp = NULL WHERE id = :user_id AND otp = :otp")?
        .execute(named_params! { ":user_id": user_id, ":otp": data.otp })?;

    Ok(cookies)
}
