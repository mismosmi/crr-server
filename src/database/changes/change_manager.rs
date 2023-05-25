use std::sync::Arc;

use tokio::sync::broadcast::{self, error::SendError};

use crate::{database::Database, error::CRRError};

use super::{DatabaseHandle, Message, Signal, Subscription};

#[derive(Clone)]
pub(crate) struct ChangeManager(
    Arc<tokio::sync::RwLock<std::collections::HashMap<String, DatabaseHandle>>>,
);

impl ChangeManager {
    pub(crate) fn new() -> Self {
        let handles = Arc::new(tokio::sync::RwLock::new(std::collections::HashMap::<
            String,
            DatabaseHandle,
        >::new()));

        // garbage collection for database handles
        let gc_handles = Arc::downgrade(&handles);

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(tokio::time::Duration::from_secs(240)).await;

                match gc_handles.upgrade() {
                    Some(gc_handles) => {
                        let lock = gc_handles.write().await;

                        let collect = Vec::new();
                        for (db_name, handle) in lock.iter() {
                            if handle.is_orphan() {
                                collect.push(db_name.to_owned());
                            }
                        }

                        for db_name in collect.into_iter() {
                            lock.remove(&db_name);
                        }
                    }
                    None => return,
                }
            }
        });

        Self(handles)
    }

    pub(crate) async fn subscribe(&self, database: Database) -> Result<Subscription, CRRError> {
        if let Some(handle) = self.0.read().await.get(database.name()) {
            handle
                .send_signal(Signal::SetDBVersion(database.db_version()))
                .await?;

            return Ok(handle.subscribe());
        }

        self.add_handle(database).await
    }

    async fn add_handle(&self, mut database: Database) -> Result<Subscription, CRRError> {
        let (message_sender, message_receiver) = tokio::sync::broadcast::channel::<Message>(32);
        let (signal_sender, mut signal_receiver) = tokio::sync::mpsc::channel::<Signal>(32);

        let hook_signal_sender = signal_sender.downgrade();

        database.update_hook(Some(
            move |_action, _arg1: &'_ str, _arg2: &'_ str, _rowid| {
                if let Some(sender) = hook_signal_sender.upgrade() {
                    let _ = sender.try_send(Signal::Update);
                }
            },
        ));

        let task_message_sender = message_sender.clone();

        let database_name = database.name().to_owned();

        tokio::spawn(async move {
            if let Err(_) = Self::send_changes(&mut database, &task_message_sender) {
                return;
            }

            while let Some(signal) = signal_receiver.recv().await {
                match signal {
                    Signal::Update => {
                        if let Err(_) = Self::send_changes(&mut database, &task_message_sender) {
                            return;
                        }
                    }
                    Signal::SetDBVersion(db_version) => {
                        if db_version < database.db_version() {
                            // clear queue
                            while task_message_sender.len() > 0 {
                                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
                            }

                            // set back db_version
                            database.set_db_version(db_version);

                            if let Err(_) = Self::send_changes(&mut database, &task_message_sender)
                            {
                                return;
                            }
                        }
                    }
                }
            }
        });

        self.0.write().await.insert(
            database_name,
            DatabaseHandle::from(message_sender, signal_sender),
        );

        Ok(message_receiver)
    }

    fn send_changes(
        database: &mut Database,
        sender: &broadcast::Sender<Message>,
    ) -> Result<(), SendError<Message>> {
        for message in database.all_changes() {
            sender.send(message.into())?;
        }

        Ok(())
    }

    pub(crate) async fn kill_connection(&self, db_name: &str) {
        if let Some(handle) = self.0.write().await.remove(db_name) {
            tracing::info!(
                "Killing {} open streams from database \"{}\"",
                handle.connection_count(),
                db_name,
            );
        }
    }
}
