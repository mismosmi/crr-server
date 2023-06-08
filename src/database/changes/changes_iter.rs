use std::fmt::Debug;

use crate::error::CRRError;

use super::Changeset;

pub(crate) struct ChangesIter<F>
where
    F: FnMut() -> Result<(Vec<Changeset>, bool), CRRError> + Send,
{
    load_page: std::sync::Mutex<F>,
    current_page: <Vec<Changeset> as IntoIterator>::IntoIter,
    has_next_page: bool,
}

impl<F> ChangesIter<F>
where
    F: FnMut() -> Result<(Vec<Changeset>, bool), CRRError> + Send,
{
    pub(crate) fn new(load_page: F) -> Self {
        Self {
            load_page: std::sync::Mutex::new(load_page),
            current_page: Vec::new().into_iter(),
            has_next_page: true,
        }
    }
}

impl<F> Debug for ChangesIter<F>
where
    F: FnMut() -> Result<(Vec<Changeset>, bool), CRRError> + Send,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut s = f.debug_struct("ChangesIter");
        s.field("load_page", &"<Function>".to_string());
        s.field("current_page", &self.current_page);
        s.field("has_nex_page", &self.has_next_page);
        s.finish()?;

        Ok(())
    }
}

impl<F> Iterator for ChangesIter<F>
where
    F: FnMut() -> Result<(Vec<Changeset>, bool), CRRError> + Send,
{
    type Item = Result<Changeset, CRRError>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(changeset) = self.current_page.next() {
            return Some(Ok(changeset));
        }

        if self.has_next_page {
            match self
                .load_page
                .lock()
                .map_err(|_| CRRError::PoisonedLockError("ChangesIter::next"))
                .and_then(|mut lock| lock())
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
