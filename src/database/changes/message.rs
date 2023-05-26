use crate::error::HttpError;

use super::Changeset;

pub(crate) type Message = Result<Changeset, HttpError>;
