mod change_manager;
mod changeset;
mod database_handle;
mod message;
mod signal;
mod stream;

pub(crate) use change_manager::ChangeManager;
pub(crate) use changeset::Changeset;
pub(crate) use database_handle::{DatabaseHandle, Subscription};
pub(crate) use message::Message;
pub(crate) use signal::Signal;

#[cfg(test)]
mod tests {

    use crate::{
        database::{changes::change_manager::ChangeManager, migrate::tests::setup_foo, Value},
        error::CRRError,
        tests::TestEnv,
    };

    #[test]
    fn list_changes() {
        let env = TestEnv::new();
        setup_foo(&env);

        let mut db = env.db();

        {
            let mut changes = db.all_changes();
            assert!(changes.next().is_none());
        }

        db.execute("INSERT INTO foo (bar) VALUES (?)", ["baz"])
            .expect("failed to insert row");

        {
            let mut changes = db.all_changes();
            let changeset = changes
                .next()
                .expect("no changes registered")
                .expect("error fetching changes");

            assert_eq!(changeset.table, "foo");
            assert_eq!(changeset.val, Value::text("baz"));
            assert_eq!(changeset.db_version, 1);

            assert!(changes.next().is_none());
        }

        db.execute("INSERT INTO foo (bar) VALUES (?)", ["foobar"])
            .expect("failed to insert row");

        {
            let mut changes = db.all_changes();
            let changeset = changes
                .next()
                .expect("no changes registered")
                .expect("error fetching changes");

            assert_eq!(changeset.val, Value::text("foobar"));
            assert_eq!(changeset.db_version, 2);
            assert!(changes.next().is_none());
        }
    }

    #[tokio::test]
    async fn react_to_changes() {
        let env = TestEnv::new();
        setup_foo(&env);

        let change_manager = ChangeManager::new(env.meta());

        let mut sub = change_manager
            .subscribe(env.db())
            .await
            .expect("Failed to set up subscription");

        env.db()
            .execute("INSERT INTO foo (bar) VALUES (?)", ["baz"])
            .expect("Failed to insert data");

        let changeset = sub
            .recv()
            .await
            .expect("Failed to receive update")
            .expect("Failed to retrieve updates");

        println!("{:?}", &changeset);

        assert_eq!(changeset.table, "foo")
    }

    #[test]
    fn sync_changes_to_database() {
        let env_a = TestEnv::new();
        setup_foo(&env_a);
        let env_b = TestEnv::new();

        let mut db_a = env_a.db();

        db_a.execute("INSERT INTO foo (bar) VALUES ('baz')", [])
            .expect("Failed to insert stuff");

        let changes = db_a
            .all_changes()
            .collect::<Result<Vec<Changeset>, CRRError>>()
            .expect("Failed to retrieve changes");

        let mut db_b = env_b.db();
        db_b.apply_changes(changes)
            .expect("Failed to apply changes");

        let baz: String = db_b
            .query_row("SELECT bar FROM foo", [], |row| row.get(0))
            .expect("Failed to select bar from foo");

        assert_eq!(baz, "baz");
    }
}
