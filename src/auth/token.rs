use rusqlite::named_params;

use crate::{error::Error, metadata::Metadata};

#[derive(rocket::FromForm)]
pub(crate) struct TokenRequestData {
    otp: Option<String>,
}

#[rocket::post("/token", data = "<data>")]
pub(crate) fn token(
    data: rocket::form::Form<TokenRequestData>,
    cookies: &rocket::http::CookieJar<'_>,
) -> Result<(), Error> {
    let metadata = Metadata::open()?;

    let user_id: i64 = match data.otp.as_ref() {
        Some(otp) => metadata
            .prepare("SELECT id FROM users WHERE otp = :otp")?
            .query_row(named_params! { ":otp": otp }, |row| row.get(0))?,

        None => {
            let token = cookies
                .get(super::COOKIE_NAME)
                .ok_or(Error::Unauthorized("Token Not Found".to_owned()))?
                .value();

            metadata
                .prepare("SELECT user_id FROM tokens WHERE token = :token AND expires > 'now'")?
                .query_row(named_params! { ":token": token }, |row| row.get(0))?
        }
    };

    {
        let token = nanoid::nanoid!();

        metadata.prepare("INSERT INTO tokens (user_id, token, expires) VALUES (:user_id, :token, JULIANDAY('now') + 400)")?
            .insert(named_params! { ":user_id": user_id, ":token": token })?;

        let cookie = rocket::http::Cookie::build(super::COOKIE_NAME, token)
            .http_only(true)
            .max_age(rocket::time::Duration::days(400))
            .same_site(rocket::http::SameSite::Strict)
            .secure(true)
            .path("/")
            .finish();

        cookies.add(cookie);
    }

    metadata
        .prepare("UPDATE users SET otp = NULL WHERE id = :user_id AND otp = :otp")?
        .execute(named_params! { ":user_id": user_id, ":otp": data.otp })?;

    Ok(())
}
