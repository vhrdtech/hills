use rkyv::validation::{validators::DefaultValidatorError, CheckArchiveError};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum IndexError {
    #[error("check_archived_root failed: {}", .0)]
    RkyvCheckArchivedRoot(String),

    #[error("Duplicate key({}) in an indexer that does not allow it", .0)]
    Duplicate(String),

    #[error("RwLock failed")]
    RwLock,

    #[error("{}", .0)]
    Other(String),
}

impl<T: Debug> From<CheckArchiveError<T, DefaultValidatorError>> for IndexError {
    fn from(value: CheckArchiveError<T, DefaultValidatorError>) -> Self {
        IndexError::RkyvCheckArchivedRoot(format!("{value:?}"))
    }
}
