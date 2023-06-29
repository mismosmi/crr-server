use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::{
    extract::{Query, State},
    Json,
};
use base64::{engine::general_purpose::STANDARD as base64, Engine};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use url::Url;

use crate::{app_state::AppState, error::CRRError};

use super::{AuthDatabase, Token};

#[derive(Deserialize)]
pub(crate) struct GetSignedUrlQuery {
    url: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SignedUrlResponse {
    hash: String,
    signed_url: String,
}

pub(crate) async fn get_signed_url(
    Query(query): Query<GetSignedUrlQuery>,
    Token(token): Token,
    State(state): State<AppState>,
) -> Result<Json<SignedUrlResponse>, CRRError> {
    let auth = AuthDatabase::open(state.env().clone())?;
    let token_id = auth.get_token_id(&token)?;
    let mut url = url::Url::parse(&query.url)?;
    let expiration = SystemTime::now() + Duration::from_secs(100);
    let expiration = expiration
        .duration_since(UNIX_EPOCH)
        .expect("This Server should not be run before the Unix Epoch")
        .as_secs();

    url.query_pairs_mut()
        .append_pair("crr-url-token-id", &token_id.to_string())
        .append_pair("crr-url-expires", &expiration.to_string())
        .append_pair("crr-url-nonce", &nanoid::nanoid!());

    let mut hasher = Sha256::new();
    hasher.update(&query.url);
    hasher.update(&token);

    let hash = base64.encode(hasher.finalize());

    url.query_pairs_mut().append_pair("crr-url-hash", &hash);

    Ok(Json(SignedUrlResponse {
        signed_url: url.as_str().to_owned(),
        hash,
    }))
}

#[derive(Deserialize)]
pub(crate) struct SignedRequestQuery {
    crr_url_hash: String,
    crr_url_token_id: i64,
    crr_url_expires: u64,
}

impl SignedRequestQuery {
    pub(crate) fn validate(&self, auth: &AuthDatabase, url: Url) -> Result<String, CRRError> {
        let query_without_hash = url
            .query_pairs()
            .filter(|(key, _value)| key != "crr-url-hash");

        let mut url = url.clone();
        url.query_pairs_mut()
            .clear()
            .extend_pairs(query_without_hash);

        let token = auth.get_token_by_id(self.crr_url_token_id)?;

        let mut hasher: Sha256 = Sha256::new();
        hasher.update(url.as_str());
        hasher.update(&token);

        // this sucks (we should decode the url hash to compare instead of encoding this one)
        // but I don't get how this GenericArray stuff from the digest lib works
        let hash = base64.encode(hasher.finalize());

        if hash != self.crr_url_hash {
            return Err(CRRError::Unauthorized(
                "Invalid Signed Request Hash".to_owned(),
            ));
        }

        let expiration = UNIX_EPOCH + Duration::from_secs(self.crr_url_expires);

        if expiration < SystemTime::now() {
            return Err(CRRError::Unauthorized("Signed URL Expired".to_owned()));
        }

        Ok(token)
    }
}
