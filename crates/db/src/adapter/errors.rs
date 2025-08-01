use kv::*;

/// Errors that can occur when creating a model.
#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum CreateModelError {
  /// A model with that ID already exists.
  ///
  /// If we're properly creating IDs randomly, this should be very rare. It's
  /// more likely that the same instance of a model is being inserted
  /// multiple times.
  #[error("model with that ID already exists")]
  ModelAlreadyExists,
  /// An index with that value already exists.
  ///
  /// This is a constraint violation, and should be handled by the caller. It
  /// means that one of the indices, listed in the model's
  /// [`UNIQUE_INDICES`](model::Model::UNIQUE_INDICES) constant, already
  /// exists in the database.
  #[error("index {index_name:?} with value \"{index_value}\" already exists")]
  UniqueIndexAlreadyExists {
    /// The name of the index.
    index_name:  String,
    /// The value of the index.
    index_value: EitherSlug,
  },
  /// An error occurred while deserializing or serializing the model.
  ///
  /// This is a bug. Since we're serializing and deserializing to
  /// messagepack, it's most likely that this results from an improper
  /// deserialization caused by trying to deserialize to the wrong type.
  #[error("failed to deserialize or serialize model")]
  #[diagnostic_source]
  Serde(miette::Report),
  /// A retryable transaction error occurred.
  ///
  /// This is not a bug, but a transient error. It should be retried.
  #[error("retryable transaction error: {0}")]
  #[diagnostic_source]
  RetryableTransaction(miette::Report),
  /// A database error occurred.
  ///
  /// THis is an unknown error. Something we didn't expect to fail failed.
  #[error("db error: {0}")]
  #[diagnostic_source]
  Db(miette::Report),
}

/// Errors that can occur when fetching a model.
#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum FetchModelError {
  /// An error occurred while deserializing or serializing the model.
  ///
  /// This is a bug. Since we're serializing and deserializing to
  /// messagepack, it's most likely that this results from an improper
  /// deserialization caused by trying to deserialize to the wrong type.
  #[error("failed to deserialize or serialize model")]
  #[diagnostic_source]
  Serde(miette::Report),
  /// A retryable transaction error occurred.
  ///
  /// This is not a bug, but a transient error. It should be retried.
  #[error("retryable transaction error: {0}")]
  #[diagnostic_source]
  RetryableTransaction(miette::Report),
  /// A database error occurred.
  ///
  /// THis is an unknown error. Something we didn't expect to fail failed.
  #[error("db error: {0}")]
  #[diagnostic_source]
  Db(miette::Report),
}

/// Errors that can occur when fetching a model by index.
#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum FetchModelByIndexError {
  /// The index does not exist.
  ///
  /// This is a usage bug. We should only be fetching by indices that are
  /// defined in the model's [`UNIQUE_INDICES`](model::Model::UNIQUE_INDICES)
  /// constant.
  #[error("index {index_name:?} does not exist")]
  IndexDoesNotExistOnModel {
    /// The name of the index.
    index_name: String,
  },
  /// The index is malformed.
  ///
  /// This means that the index exists and points to an ID, but the model
  /// with that ID does not exist. This is a bug, because we should be
  /// cleaning up old indices when models are deleted.
  #[error("index {index_name:?} is malformed")]
  IndexMalformed {
    /// The name of the index.
    index_name:  String,
    /// The value of the index.
    index_value: EitherSlug,
  },
  /// An error occurred while fetching the indexed model.
  ///
  /// This means after we fetched the index, we tried to fetch the model by
  /// its ID and failed.
  #[error("failed to fetch indexed model")]
  #[diagnostic_source]
  FailedToFetchIndexedModel(#[from] FetchModelError),
  /// An error occurred while deserializing or serializing the model.
  ///
  /// This is a bug. Since we're serializing and deserializing to
  /// messagepack, it's most likely that this results from an improper
  /// deserialization caused by trying to deserialize to the wrong type.
  #[error("failed to deserialize or serialize model")]
  #[diagnostic_source]
  Serde(miette::Report),
  /// A retryable transaction error occurred.
  ///
  /// This is not a bug, but a transient error. It should be retried.
  #[error("retryable transaction error: {0}")]
  #[diagnostic_source]
  RetryableTransaction(miette::Report),
  /// A database error occurred.
  ///
  /// This is an unknown error. Something we didn't expect to fail failed.
  #[error("db error: {0}")]
  #[diagnostic_source]
  Db(miette::Report),
}

/// Errors that can occur when patching a model.
#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum PatchModelError {
  /// The model with the specified ID was not found.
  ///
  /// This means we tried to update a model that doesn't exist in the database.
  /// The caller should check if the model exists before attempting to patch
  /// it.
  #[error("model with ID not found")]
  ModelNotFound,
  /// An index with that value already exists.
  ///
  /// This is a constraint violation that occurs when the patch would create
  /// a duplicate unique index. One of the indices, listed in the model's
  /// [`UNIQUE_INDICES`](model::Model::UNIQUE_INDICES) constant, already
  /// exists in the database with the new value.
  #[error("index {index_name:?} with value \"{index_value}\" already exists")]
  UniqueIndexAlreadyExists {
    /// The name of the index.
    index_name:  String,
    /// The value of the index.
    index_value: EitherSlug,
  },
  /// An error occurred while deserializing or serializing the model.
  ///
  /// This is a bug. Since we're serializing and deserializing to
  /// messagepack, it's most likely that this results from an improper
  /// deserialization caused by trying to deserialize to the wrong type.
  #[error("failed to deserialize or serialize model")]
  #[diagnostic_source]
  Serde(miette::Report),
  /// A retryable transaction error occurred.
  ///
  /// This is not a bug, but a transient error. It should be retried.
  #[error("retryable transaction error: {0}")]
  #[diagnostic_source]
  RetryableTransaction(miette::Report),
  /// A database error occurred.
  ///
  /// This is an unknown error. Something we didn't expect to fail failed.
  #[error("db error: {0}")]
  #[diagnostic_source]
  Db(miette::Report),
}

/// Errors that can occur when deleting a model.
#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum DeleteModelError {
  /// An error occurred while cleaning up indices.
  ///
  /// This is a consistency bug. The model was found and deleted, but we
  /// failed to clean up one or more of its indices. This could leave
  /// dangling index references in the database.
  #[error("failed to clean up indices for deleted model")]
  #[diagnostic_source]
  FailedToCleanupIndices(miette::Report),
  /// An error occurred while deserializing or serializing the model.
  ///
  /// This is a bug. Since we're serializing and deserializing to
  /// messagepack, it's most likely that this results from an improper
  /// deserialization caused by trying to deserialize to the wrong type.
  #[error("failed to deserialize or serialize model")]
  #[diagnostic_source]
  Serde(miette::Report),
  /// A retryable transaction error occurred.
  ///
  /// This is not a bug, but a transient error. It should be retried.
  #[error("retryable transaction error: {0}")]
  #[diagnostic_source]
  RetryableTransaction(miette::Report),
  /// A database error occurred.
  ///
  /// This is an unknown error. Something we didn't expect to fail failed.
  #[error("db error: {0}")]
  #[diagnostic_source]
  Db(miette::Report),
}
