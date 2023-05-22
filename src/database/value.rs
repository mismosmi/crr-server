use rusqlite::{
    types::{FromSql, ToSqlOutput, ValueRef},
    ToSql,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::CRRError;

#[derive(Clone, Serialize, Debug, PartialEq, Deserialize)]
pub(crate) enum Value {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    pub(crate) fn size(&self) -> usize {
        match self {
            Self::Null => 0,
            Self::Integer(_) => 8,
            Self::Real(_) => 8,
            Self::Text(value) => value.len(),
            Self::Blob(value) => value.len(),
        }
    }

    #[cfg(test)]
    pub(crate) fn text(value: &str) -> Self {
        Self::Text(format!("'{}'", value))
    }
}

impl FromSql for Value {
    fn column_result(value: ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
        use rusqlite::types::Value as RusqliteValue;

        let value: RusqliteValue = value.into();
        match value {
            RusqliteValue::Blob(value) => Ok(Self::Blob(value)),
            RusqliteValue::Integer(value) => Ok(Self::Integer(value)),
            RusqliteValue::Real(value) => Ok(Self::Real(value)),
            RusqliteValue::Null => Ok(Self::Null),
            RusqliteValue::Text(value) => Ok(Self::Text(value)),
        }
    }
}

impl ToSql for Value {
    fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
        match self {
            Self::Blob(value) => Ok(ToSqlOutput::Borrowed(ValueRef::Blob(&value))),
            Self::Integer(value) => Ok(ToSqlOutput::Borrowed(ValueRef::Integer(value.clone()))),
            Self::Real(value) => Ok(ToSqlOutput::Borrowed(ValueRef::Real(value.clone()))),
            Self::Null => Ok(ToSqlOutput::Borrowed(ValueRef::Null)),
            Self::Text(value) => Ok(ToSqlOutput::Borrowed(ValueRef::Text(value.as_bytes()))),
        }
    }
}

impl TryFrom<serde_json::Value> for Value {
    type Error = CRRError;

    fn try_from(value: serde_json::Value) -> Result<Self, Self::Error> {
        match value {
            serde_json::Value::Null => Ok(Self::Null),
            serde_json::Value::Array(_) => Err(CRRError::ParameterArrayTypeError),
            serde_json::Value::Object(_) => Err(CRRError::ParameterObjectTypeError),
            serde_json::Value::Bool(value) => Ok(Self::Integer(if value { 1 } else { 0 })),
            serde_json::Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    return Ok(Self::Integer(value));
                }

                if let Some(value) = value.as_f64() {
                    return Ok(Self::Real(value));
                }

                Err(CRRError::ParameterNumberTypeError)
            }
            serde_json::Value::String(value) => Ok(Self::Text(value)),
        }
    }
}
