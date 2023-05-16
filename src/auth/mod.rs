pub(crate) mod database;
pub(crate) mod otp;
pub(crate) mod table;
pub(crate) mod token;

use rusqlite::named_params;

use crate::{error::CRRError, metadata::Metadata};

pub(crate) const COOKIE_NAME: &'static str = "CRR_TOKEN";

pub(crate) struct User {
    id: i64,
}

impl User {
    pub(crate) fn authenticate(
        meta: &Metadata,
        cookies: &rocket::http::CookieJar,
    ) -> Result<Self, CRRError> {
        let id: i64 = meta.prepare("SELECT user_id FROM tokens WHERE token = :token AND expires > 'now'")?
        .query_row(named_params! {
            ":token": cookies.get(COOKIE_NAME).ok_or(CRRError::Unauthorized("No Token Found".to_string()))?.value()
        }, |row| row.get(0))?;

        Ok(Self { id })
    }
}
