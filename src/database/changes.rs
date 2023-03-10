use rocket::{
    http::CookieJar,
    response::stream::{Event, EventStream},
    tokio, State,
};
use rusqlite::named_params;

use crate::{auth::User, error::Error};

use super::{ChangeMessage, Changeset, Database};

const CHANGE_BUFFER_SIZE: usize = 1_000_000;

#[rocket::get("/<database>/changes?<site_id>&<db_version>")]
pub(crate) async fn changes<'s, 'c, 'i>(
    database: String,
    site_id: &'i str,
    db_version: i64,
    change_manager: &'s State<ChangeManager>,
    cookies: &'c CookieJar<'c>,
) -> EventStream![Event + 's]
where
    'i: 's,
    'c: 's,
{
    EventStream! {
        let db = Database::open_readonly(database.clone(), db_version);

        if let Err(error) = db {
            yield Event::json(&error);
            return
        }

        let mut db = db.unwrap();

        let user = User::authenticate(cookies);

        if let Err(error) = user {
            yield Event::json(&error);
            return
        }

        let user = user.unwrap();

        let changes = db.changes(&user, site_id);

        if let Err(error) = changes {
            yield Event::json(&error);
            return
        }

        let changes = changes.unwrap();

        for changeset in changes {
            match changeset {
                Ok(changeset) => {
                    yield Event::json(&changeset)
                },
                Err(error) => {
                    yield Event::json(&error);
                    return
                }
            }
        }

        let subscription = change_manager.subscribe(db).await;

        if let Err(error) = subscription {
            yield Event::json(&error);
            return
        }

        let mut subscription = subscription.unwrap();

        while let Ok(changeset) = subscription.recv().await {
            match changeset {
                Ok(changeset) => {
                    yield Event::json(&changeset)
                },
                Err(error) => {
                    yield Event::json(&error);
                    return
                }
            }
        }
    }
}

enum UpdateMessage {
    Update,
    Close,
}

pub(crate) struct ChangeManager {
    handles: tokio::sync::RwLock<
        std::collections::HashMap<
            String,
            (
                tokio::sync::broadcast::Sender<ChangeMessage>,
                tokio::sync::mpsc::Sender<UpdateMessage>,
            ),
        >,
    >,
}

impl ChangeManager {
    pub(crate) fn new() -> Self {
        Self {
            handles: tokio::sync::RwLock::new(std::collections::HashMap::new()),
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
        let (update_sender, update_receiver) = tokio::sync::mpsc::channel::<UpdateMessage>(32);

        let hook_update_sender = update_sender.clone();

        database.update_hook(Some(
            move |_action, _arg1: &'_ str, _arg2: &'_ str, _rowid| {
                let _err = hook_update_sender.try_send(UpdateMessage::Update);
            },
        ));

        async fn process_changes(
            mut db: super::Database,
            mut update_receiver: tokio::sync::mpsc::Receiver<UpdateMessage>,
            changes_sender: tokio::sync::broadcast::Sender<Result<Changeset, Error>>,
        ) -> Result<(), Error> {
            for changeset in db.all_changes() {
                changes_sender.send(changeset)?;
            }

            while let Some(msg) = update_receiver.recv().await {
                match msg {
                    UpdateMessage::Update => {
                        for changeset in db.all_changes() {
                            changes_sender.send(changeset)?;
                        }
                    }
                    UpdateMessage::Close => {
                        if changes_sender.receiver_count() == 0 {
                            return Ok(());
                        }
                    }
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
    update_sender: tokio::sync::mpsc::Sender<UpdateMessage>,
}

impl std::ops::Drop for Subscription {
    fn drop(&mut self) {
        let _err = self.update_sender.try_send(UpdateMessage::Close);
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

impl Database {
    fn changes<'d, 's>(
        &'d mut self,
        user: &User,
        site_id: &'s str,
    ) -> Result<ChangesIter<impl FnMut() -> Result<(Vec<Changeset>, bool), Error> + 'd>, Error>
    where
        's: 'd,
    {
        let allowed_tables = user.readable_tables(self.name())?;

        Ok(ChangesIter::new(move || {
            let query = "
                SELECT table, pk, cid, val, col_version, db_version, site_id
                FROM crsql_changes
                WHERE db_version > :db_version
                AND site_id <> :site_id
                AND table IN :allowed_tables
            ";

            let mut buffer = Vec::<Changeset>::new();
            let mut db_version = 0i64;
            let mut has_next_page = false;

            {
                let mut buffer_size = 0usize;

                let mut stmt = self.prepare(query)?;

                let mut rows = stmt.query(named_params! {
                    ":db_version": self.db_version,
                    ":site_id": site_id,
                    ":allowed_tables": allowed_tables,
                })?;

                while let Ok(Some(row)) = rows.next() {
                    let changeset: Changeset = row.try_into()?;

                    db_version = changeset.db_version();
                    buffer_size += changeset.size();

                    buffer.push(changeset);

                    if buffer_size > CHANGE_BUFFER_SIZE {
                        has_next_page = true;
                        break;
                    }
                }
            }

            self.db_version = db_version;
            Ok((buffer, has_next_page))
        }))
    }

    fn all_changes<'d>(
        &'d mut self,
    ) -> ChangesIter<impl FnMut() -> Result<(Vec<Changeset>, bool), Error> + 'd> {
        ChangesIter::new(move || {
            let query = "
                SELECT table, pk, cid, val, col_version, db_version, site_id
                FROM crsql_changes
                WHERE db_version > ?
            ";

            let mut buffer = Vec::<Changeset>::new();
            let mut db_version = 0i64;
            let mut has_next_page = false;

            {
                let mut buffer_size = 0usize;
                let mut stmt = self.conn.prepare(query)?;
                let mut rows = stmt.query([&db_version])?;

                while let Some(row) = rows.next()? {
                    let changeset: Changeset = row.try_into()?;
                    db_version = changeset.db_version;
                    buffer_size += changeset.size();

                    buffer.push(changeset);

                    if buffer_size > CHANGE_BUFFER_SIZE {
                        has_next_page = true;
                        break;
                    }
                }
            }

            Ok((buffer, has_next_page))
        })
    }
}

struct ChangesIter<F>
where
    F: FnMut() -> Result<(Vec<Changeset>, bool), Error> + Send,
{
    load_page: std::sync::Mutex<F>,
    current_page: <Vec<Changeset> as IntoIterator>::IntoIter,
    has_next_page: bool,
}

impl<F> ChangesIter<F>
where
    F: FnMut() -> Result<(Vec<Changeset>, bool), Error> + Send,
{
    fn new(load_page: F) -> Self {
        Self {
            load_page: std::sync::Mutex::new(load_page),
            current_page: Vec::new().into_iter(),
            has_next_page: true,
        }
    }
}

impl<F> Iterator for ChangesIter<F>
where
    F: FnMut() -> Result<(Vec<Changeset>, bool), Error> + Send,
{
    type Item = Result<Changeset, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(changeset) = self.current_page.next() {
            return Some(Ok(changeset));
        }

        if self.has_next_page {
            match self
                .load_page
                .lock()
                .map_err(|_| Error::ServerError("Poisoned lock in ChangesIter".to_owned()))
                .and_then(|mut lock| Ok(lock()?))
            {
                Ok((page, has_next_page)) => {
                    self.current_page = page.into_iter();
                    self.has_next_page = has_next_page;
                    return self.current_page.next().map(|changeset| Ok(changeset));
                }
                Err(error) => return Some(Err(error)),
            }
        }

        None
    }
}
