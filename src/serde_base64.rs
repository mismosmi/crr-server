use base64::{engine::general_purpose::STANDARD as base64, Engine};
use rocket::serde::{Deserialize, Deserializer, Serialize, Serializer};

pub fn serialize<S: Serializer>(v: &Vec<u8>, s: S) -> Result<S::Ok, S::Error> {
    let encoded = base64.encode(v);
    String::serialize(&encoded, s)
}

pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
    let encoded = String::deserialize(d)?;
    base64
        .decode(encoded.as_bytes())
        .map_err(|e| rocket::serde::de::Error::custom(e))
}
