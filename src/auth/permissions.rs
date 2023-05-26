use std::collections::HashMap;

use crate::error::CRRError;

#[derive(Default, Debug, Clone, Copy)]
pub(crate) struct PartialPermissions {
    pub(crate) read: bool,
    pub(crate) insert: bool,
    pub(crate) update: bool,
    pub(crate) delete: bool,
}

impl PartialPermissions {
    fn is_empty(&self) -> bool {
        return !self.read && !self.insert && !self.update && !self.delete;
    }
}

#[derive(Debug, Clone)]
pub(crate) enum ObjectPermissions {
    Full,
    Partial(PartialPermissions),
}

impl Default for ObjectPermissions {
    fn default() -> Self {
        Self::Partial(PartialPermissions::default())
    }
}

impl ObjectPermissions {
    pub(crate) fn set_full(&mut self) {
        *self = Self::Full;
    }

    pub(crate) fn set(&mut self, permissions: PartialPermissions) {
        match self {
            Self::Full => (),
            Self::Partial(p) => *p = permissions,
        }
    }

    pub(crate) fn full(&self) -> bool {
        match self {
            Self::Full => true,
            _ => false,
        }
    }
    pub(crate) fn read(&self) -> bool {
        match self {
            Self::Full => true,
            Self::Partial(p) => p.read,
        }
    }
    pub(crate) fn insert(&self) -> bool {
        match self {
            Self::Full => true,
            Self::Partial(p) => p.insert,
        }
    }
    pub(crate) fn update(&self) -> bool {
        match self {
            Self::Full => true,
            Self::Partial(p) => p.update,
        }
    }
    pub(crate) fn delete(&self) -> bool {
        match self {
            Self::Full => true,
            Self::Partial(p) => p.delete,
        }
    }
}

#[derive(Clone)]
pub(crate) enum DatabasePermissions {
    Full,
    Partial {
        database: PartialPermissions,
        tables: HashMap<String, ObjectPermissions>,
    },
}

impl Default for DatabasePermissions {
    fn default() -> Self {
        Self::Partial {
            database: PartialPermissions::default(),
            tables: HashMap::new(),
        }
    }
}

impl DatabasePermissions {
    pub(crate) fn set_full(&mut self) {
        *self = Self::Full;
    }
    pub(crate) fn set(&mut self, permissions: PartialPermissions) {
        match self {
            Self::Full => (),
            Self::Partial { database, .. } => {
                *database = permissions;
            }
        }
    }

    fn with_table<F>(&mut self, table_name: String, f: F)
    where
        F: FnOnce(&mut ObjectPermissions),
    {
        match self {
            Self::Full => (),
            Self::Partial { tables, .. } => {
                let table = tables
                    .entry(table_name)
                    .or_insert(ObjectPermissions::default());

                f(table)
            }
        }
    }

    pub(crate) fn set_table_full(&mut self, table_name: String) {
        self.with_table(table_name, |t| t.set_full());
    }
    pub(crate) fn set_table(&mut self, table_name: String, permissions: PartialPermissions) {
        self.with_table(table_name, |t| t.set(permissions));
    }

    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::Full => false,
            Self::Partial { database, tables } => {
                return database.is_empty() && tables.is_empty();
            }
        }
    }

    pub(crate) fn apply<F>(&self, mut f: F) -> Result<(), CRRError>
    where
        F: FnMut(Option<&str>, ObjectPermissions) -> Result<(), CRRError>,
    {
        match self {
            Self::Full => f(None, ObjectPermissions::Full),
            Self::Partial { database, tables } => {
                f(None, ObjectPermissions::Partial(database.clone()))?;

                for (table_name, permissions) in tables {
                    f(Some(table_name), permissions.clone())?;
                }

                Ok(())
            }
        }
    }

    pub(crate) fn full(&self) -> bool {
        match self {
            Self::Full => true,
            _ => false,
        }
    }

    #[cfg(test)]
    pub(crate) fn read(&self) -> bool {
        match self {
            Self::Full => true,
            Self::Partial { database, .. } => database.read,
        }
    }
    #[cfg(test)]
    pub(crate) fn insert(&self) -> bool {
        match self {
            Self::Full => true,
            Self::Partial { database, .. } => database.insert,
        }
    }
    #[cfg(test)]
    pub(crate) fn full_table(&self, table_name: &str) -> bool {
        match self {
            Self::Full => true,
            Self::Partial { tables, .. } => {
                tables.get(table_name).map(|p| p.full()).unwrap_or(false)
            }
        }
    }
    pub(crate) fn read_table(&self, table_name: &str) -> bool {
        match self {
            Self::Full => true,
            Self::Partial { database, tables } => {
                database.read || tables.get(table_name).map(|p| p.read()).unwrap_or(false)
            }
        }
    }
    pub(crate) fn update_table(&self, table_name: &str) -> bool {
        match self {
            Self::Full => true,
            Self::Partial { database, tables } => {
                database.update || tables.get(table_name).map(|p| p.update()).unwrap_or(false)
            }
        }
    }
    pub(crate) fn insert_table(&self, table_name: &str) -> bool {
        match self {
            Self::Full => true,
            Self::Partial { database, tables } => {
                database.insert || tables.get(table_name).map(|p| p.insert()).unwrap_or(false)
            }
        }
    }
    pub(crate) fn delete_table(&self, table_name: &str) -> bool {
        match self {
            Self::Full => true,
            Self::Partial { database, tables } => {
                database.delete || tables.get(table_name).map(|p| p.delete()).unwrap_or(false)
            }
        }
    }

    pub(crate) fn readable_tables(&self) -> AllowedTables {
        match self {
            Self::Full => AllowedTables::All,
            Self::Partial { database, tables } => {
                if database.read {
                    AllowedTables::All
                } else {
                    AllowedTables::Some(
                        tables
                            .keys()
                            .map(|table_name| table_name.to_owned())
                            .collect(),
                    )
                }
            }
        }
    }
}

#[derive(PartialEq, Debug)]
pub(crate) enum AllowedTables {
    All,
    Some(Vec<String>),
}

impl AllowedTables {
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            Self::All => false,
            Self::Some(tables) => tables.is_empty(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::auth::AllowedTables;

    use super::{DatabasePermissions, ObjectPermissions, PartialPermissions};

    #[test]
    fn full() {
        let p = DatabasePermissions::Full;

        assert!(p.full(), "Has full access to Database");
        assert!(p.full_table("foo"), "Has full access to arbitrary table");
        assert!(p.read(), "Has read access to Database");
        assert!(p.read_table("bar"), "Has read access to arbitrary table");
        assert!(p.insert(), "Has insert access to Database");
        assert!(
            p.delete_table("baz"),
            "Has delete access to arbitrary table"
        );

        let readable_tables = p.readable_tables();

        assert_eq!(readable_tables.is_empty(), false);
        assert_eq!(readable_tables, AllowedTables::All);
    }

    #[test]
    fn readonly() {
        let p = DatabasePermissions::Partial {
            database: PartialPermissions {
                read: true,
                insert: false,
                update: false,
                delete: false,
            },
            tables: HashMap::new(),
        };

        assert!(!p.full());
        assert!(p.read());
        assert!(!p.full_table("foo"));
        assert!(p.read_table("bar"));
        assert!(!p.insert());
        assert!(!p.update_table("baz"));

        assert_eq!(p.readable_tables(), AllowedTables::All);
    }

    #[test]
    fn read_table() {
        let mut tables = HashMap::new();
        tables.insert(
            "foo".to_owned(),
            ObjectPermissions::Partial(PartialPermissions {
                read: true,
                insert: false,
                update: false,
                delete: false,
            }),
        );
        let p = DatabasePermissions::Partial {
            database: PartialPermissions::default(),
            tables,
        };

        assert!(!p.full(), "No full permissions");
        assert!(!p.full_table("foo"), "No full table permissions");
        assert!(!p.read(), "No read permissions for whole DB");
        assert!(p.read_table("foo"), "Read permissions for table");
        assert!(!p.insert_table("foo"), "No insert permissions for table");

        assert_eq!(
            p.readable_tables(),
            AllowedTables::Some(vec!["foo".to_owned()]),
            "Table is in readable tables"
        );
    }
}
