use kv::{KvError, KvResult};

struct RedbCatchallError(miette::Report);

impl From<redb::TransactionError> for RedbCatchallError {
  fn from(error: redb::TransactionError) -> Self {
    RedbCatchallError(miette::Report::from_err(error))
  }
}

impl From<redb::TableError> for RedbCatchallError {
  fn from(error: redb::TableError) -> Self {
    RedbCatchallError(miette::Report::from_err(error))
  }
}

impl From<redb::StorageError> for RedbCatchallError {
  fn from(error: redb::StorageError) -> Self {
    RedbCatchallError(miette::Report::from_err(error))
  }
}

impl From<redb::SavepointError> for RedbCatchallError {
  fn from(error: redb::SavepointError) -> Self {
    RedbCatchallError(miette::Report::from_err(error))
  }
}

impl From<redb::CommitError> for RedbCatchallError {
  fn from(error: redb::CommitError) -> Self {
    RedbCatchallError(miette::Report::from_err(error))
  }
}

pub(crate) trait IntoKvError<T> {
  fn to_kv_err(self) -> KvResult<T>;
}

impl<T, E> IntoKvError<T> for Result<T, E>
where
  T: Sized,
  RedbCatchallError: From<E>,
{
  fn to_kv_err(self) -> KvResult<T> {
    self
      .map_err(RedbCatchallError::from)
      .map_err(|e| KvError::PlatformError(e.0))
  }
}
