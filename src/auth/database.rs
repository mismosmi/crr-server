use crate::error::Error;
use rusqlite::named_params;

impl super::User {
    pub(crate) fn owns_database(&self, database: &str) -> Result<bool, Error> {
        let mut query = self.db.prepare(
            "
            SELECT TRUE 
            FROM database_owners
            WHERE user_id = :user_id
            AND database_name = :database_name
            ",
        )?;

        let granted = query.exists(named_params! {
            ":user_id": self.id,
            ":database_name": database
        })?;

        Ok(granted)
    }
}
