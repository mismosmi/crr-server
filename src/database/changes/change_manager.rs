use rocket::tokio;

use crate::{
    database::{migrations::Migration, ChangeMessage, Changeset, Database},
    error::Error,
    metadata::Metadata,
};

pub(crate) struct ChangeManager {
    handles: tokio::sync::RwLock<
        std::collections::HashMap<
            String,
            (
                tokio::sync::broadcast::Sender<ChangeMessage>,
                tokio::sync::mpsc::Sender<()>,
            ),
        >,
    >,
    migrations: tokio::sync::broadcast::Sender<Migration>,
}

impl ChangeManager {
    pub(crate) fn new(meta: Metadata) -> Self {
        let (migrations_sender, migrations_receiver) = tokio::sync::broadcast::channel(32);

        //meta.update_hook(Some(move |_action, _, _, _| {
        //    //let _err = update_sender.try_send(());
        //}));

        async fn process_migrations(meta: Metadata) {
            let (update_sender, update_receiver) = tokio::sync::mpsc::channel::<()>(1);
        }

        Self {
            handles: tokio::sync::RwLock::new(std::collections::HashMap::new()),
            migrations: migrations_sender,
        }
    }

    pub(crate) async fn subscribe(&self, database: Database) -> Result<Subscription, Error> {
        if let Some((changes_sender, update_sender)) =
            self.handles.read().await.get(database.name())
        {
            if changes_sender.receiver_count() > 0 {
                let subscription = Subscription {
                    changes_receiver: changes_sender.subscribe(),
                    update_sender: update_sender.clone(),
                };

                return Ok(subscription);
            }
        }

        self.add_handle(database).await
    }

    async fn add_handle(&self, database: Database) -> Result<Subscription, Error> {
        let (changes_sender, changes_receiver) =
            tokio::sync::broadcast::channel::<ChangeMessage>(32);
        let (update_sender, update_receiver) = tokio::sync::mpsc::channel::<()>(1);

        let hook_update_sender = update_sender.clone();

        database.update_hook(Some(
            move |_action, _arg1: &'_ str, _arg2: &'_ str, _rowid| {
                let _err = hook_update_sender.try_send(());
            },
        ));

        async fn process_changes(
            mut db: super::Database,
            mut update_receiver: tokio::sync::mpsc::Receiver<()>,
            changes_sender: tokio::sync::broadcast::Sender<Result<Changeset, Error>>,
        ) -> Result<(), Error> {
            for changeset in db.all_changes() {
                changes_sender.send(changeset)?;
            }

            while let Some(_) = update_receiver.recv().await {
                if changes_sender.receiver_count() == 0 {
                    break;
                }

                for changeset in db.all_changes() {
                    changes_sender.send(changeset)?;
                }
            }

            Ok(())
        }

        let task_changes_sender = changes_sender.clone();

        let database_name = database.name().to_owned();

        tokio::spawn(async move {
            if let Err(error) =
                process_changes(database, update_receiver, task_changes_sender.clone()).await
            {
                let _err = task_changes_sender.send(Err(error));
            }
        });

        self.handles
            .write()
            .await
            .insert(database_name, (changes_sender, update_sender.clone()));

        Ok(Subscription {
            changes_receiver,
            update_sender,
        })
    }
}

pub(crate) struct Subscription {
    changes_receiver: tokio::sync::broadcast::Receiver<ChangeMessage>,
    update_sender: tokio::sync::mpsc::Sender<()>,
}

impl std::ops::Drop for Subscription {
    fn drop(&mut self) {
        let _err = self.update_sender.try_send(());
    }
}

impl std::ops::Deref for Subscription {
    type Target = tokio::sync::broadcast::Receiver<ChangeMessage>;

    fn deref(&self) -> &Self::Target {
        &self.changes_receiver
    }
}

impl std::ops::DerefMut for Subscription {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.changes_receiver
    }
}
