#[derive(Debug)]
pub(crate) enum Signal {
    SetDBVersion(i64),
    Update,
}
