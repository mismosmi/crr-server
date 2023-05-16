use crate::{error::CRRError, metadata::Metadata};
use rusqlite::named_params;

impl super::User {
    pub(crate) fn readable_tables(
        &self,
        meta: &Metadata,
        database_name: &str,
    ) -> Result<Vec<String>, CRRError> {
        self.tables_with_permission(meta, database_name, "pread")
    }

    //fn has_permission_for_table(
    //    &self,
    //    meta: &Metadata,
    //    database_name: &str,
    //    table_name: &str,
    //    permission: &str,
    //) -> Result<bool, Error> {
    //    let query = format!(
    //        "
    //            SELECT table_permissions.{}
    //            FROM user_roles
    //            LEFT JOIN permissions
    //            ON user_roles.role_id = table_permissions.role_id
    //            WHERE user_roles.user_id = :user_id
    //            AND database_name = :database_name
    //            AND table_name = :table_name
    //        ",
    //        permission
    //    );

    //    let granted: bool = meta.prepare(&query)?.query_row(
    //        named_params! {
    //            ":user_id": self.id,
    //            ":database_name": database_name,
    //            ":table_name": table_name
    //        },
    //        |row| row.get(0),
    //    )?;

    //    Ok(granted)
    //}

    fn tables_with_permission(
        &self,
        meta: &Metadata,
        database_name: &str,
        permission: &str,
    ) -> Result<Vec<String>, CRRError> {
        let query = format!(
            "
                SELECT permissions.table_name
                FROM user_roles
                LEFT JOIN table_permissions
                ON user_roles.role_id = table_permissions.role_id
                AND database_name = :database_name
                AND table_permissions.{} = TRUE
            ",
            permission
        );

        let mut stmt = meta.prepare(&query)?;
        let mut rows = stmt.query(named_params! {
            ":user_id": self.id,
            ":database_name": database_name
        })?;

        let mut table_names = Vec::new();

        while let Ok(Some(row)) = rows.next() {
            let table_name = row.get(0)?;
            table_names.push(table_name);
        }

        Ok(table_names)
    }
}
