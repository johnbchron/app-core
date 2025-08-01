#![feature(iterator_try_collect)]
#![feature(slice_pattern)]

//! A generic interface for key-value stores.
//!
//! Specifically, this crate is for **transactional** key-value stores. We also
//! assume that the key and value types are both byte arrays. The primary
//! interface is [`KeyValueStore`], which provides methods for beginning
//! transactions.
//!
//! The transactions themselves implement [`KvPrimitive`] and [`KvTransaction`],
//! which provide basic operations and transaction-specific operations,
//! respectively.
//!
//! Other highlights include a zero-copy segment-based key encoding scheme, and
//! optional automatic messagepack ser/de for values.
//!
//! `tikv`, `redb`, and `mock` platforms exist, governed by their respective
//! feature flags.

mod key;
#[cfg(feature = "mock")]
mod mock_impl;
#[cfg(feature = "redb")]
mod redb_impl;
mod retryable;
#[cfg(feature = "tikv")]
mod tikv_impl;
mod txn_ext;
mod value;

use std::{fmt, ops::Bound, sync::Arc};

use hex::health;
pub use slugger::*;

#[cfg(feature = "mock")]
pub use self::mock_impl::MockStore;
pub use self::{key::Key, txn_ext::KvTransactionExt, value::Value};

/// Represents errors that can occur when interacting with a key-value store.
#[derive(Debug, thiserror::Error, miette::Diagnostic)]
pub enum KvError {
  /// An error occurred in the underlying platform.
  #[error("platform error: {0}")]
  #[diagnostic(transparent)]
  PlatformError(miette::Report),
}

#[cfg(feature = "tikv")]
impl From<tikv_client::Error> for KvError {
  fn from(error: tikv_client::Error) -> Self {
    KvError::PlatformError(miette::Report::from_err(error))
  }
}

#[cfg(feature = "redb")]
mod redb_error_impl {
  use super::KvError;

  impl From<redb::TransactionError> for KvError {
    fn from(error: redb::TransactionError) -> Self {
      KvError::PlatformError(miette::Report::from_err(error))
    }
  }

  impl From<redb::TableError> for KvError {
    fn from(error: redb::TableError) -> Self {
      KvError::PlatformError(miette::Report::from_err(error))
    }
  }

  impl From<redb::StorageError> for KvError {
    fn from(error: redb::StorageError) -> Self {
      KvError::PlatformError(miette::Report::from_err(error))
    }
  }

  impl From<redb::SavepointError> for KvError {
    fn from(error: redb::SavepointError) -> Self {
      KvError::PlatformError(miette::Report::from_err(error))
    }
  }

  impl From<redb::CommitError> for KvError {
    fn from(error: redb::CommitError) -> Self {
      KvError::PlatformError(miette::Report::from_err(error))
    }
  }
}

/// Represents the result of a key-value operation.
pub type KvResult<T> = Result<T, KvError>;

/// Defines primitive methods for operating key-value stores.
#[async_trait::async_trait]
pub trait KvPrimitive {
  /// Get the value of a key.
  async fn get(&mut self, key: &Key) -> KvResult<Option<Value>>;
  /// Set the value of a key.
  async fn put(&mut self, key: &Key, value: Value) -> KvResult<()>;
  /// Set the value of a key, only if it does not exist.
  async fn insert(&mut self, key: &Key, value: Value) -> KvResult<()>;
  /// Scan the keyspace.
  async fn scan(
    &mut self,
    start: Bound<Key>,
    end: Bound<Key>,
    limit: Option<u32>,
  ) -> KvResult<Vec<(Key, Value)>>;
  /// Delete a key.
  async fn delete(&mut self, key: &Key) -> KvResult<bool>;
}

/// Defines methods on transactions.
#[async_trait::async_trait]
pub trait KvTransaction: KvPrimitive {
  /// Commit the transaction.
  async fn commit(&mut self) -> KvResult<()>;
  /// Rollback the transaction.
  async fn rollback(&mut self) -> KvResult<()>;
}

/// Defines methods and types for performing transactions on a key-value store.
#[async_trait::async_trait]
pub(crate) trait KvTransactional: hex::Hexagonal {
  /// Begin an optimistic transaction.
  async fn begin_optimistic_transaction(&self) -> KvResult<DynTransaction>;
  /// Begin a pessimistic transaction.
  async fn begin_pessimistic_transaction(&self) -> KvResult<DynTransaction>;
}

/// A dynamic transaction type.
#[must_use]
pub struct DynTransaction(Box<dyn KvTransaction + Send + Sync + 'static>);

impl DynTransaction {
  #[cfg(any(feature = "mock", feature = "redb", feature = "tikv"))]
  pub(crate) fn new<T: KvTransaction + Send + Sync + 'static>(
    inner: T,
  ) -> Self {
    Self(Box::new(inner))
  }
}

#[async_trait::async_trait]
impl KvTransaction for DynTransaction {
  async fn commit(&mut self) -> KvResult<()> { self.0.commit().await }
  async fn rollback(&mut self) -> KvResult<()> { self.0.rollback().await }
}

#[async_trait::async_trait]
impl KvPrimitive for DynTransaction {
  async fn get(&mut self, key: &Key) -> KvResult<Option<Value>> {
    self.0.get(key).await
  }

  async fn put(&mut self, key: &Key, value: Value) -> KvResult<()> {
    self.0.put(key, value).await
  }

  async fn insert(&mut self, key: &Key, value: Value) -> KvResult<()> {
    self.0.insert(key, value).await
  }

  async fn scan(
    &mut self,
    start: Bound<Key>,
    end: Bound<Key>,
    limit: Option<u32>,
  ) -> KvResult<Vec<(Key, Value)>> {
    self.0.scan(start, end, limit).await
  }

  async fn delete(&mut self, key: &Key) -> KvResult<bool> {
    self.0.delete(key).await
  }
}

/// A key-value store.
#[derive(Clone)]
pub struct KeyValueStore {
  inner: Arc<dyn KvTransactional>,
}

impl fmt::Debug for KeyValueStore {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    write!(f, "KeyValueStore")
  }
}

#[async_trait::async_trait]
impl health::HealthReporter for KeyValueStore {
  fn name(&self) -> &'static str { stringify!(HealthReporter) }
  async fn health_check(&self) -> health::ComponentHealth {
    health::AdditiveComponentHealth::from_futures(vec![self
      .inner
      .health_report()])
    .await
    .into()
  }
}

impl KeyValueStore {
  /// Create a new key-value store pointing to a TiKV instance.
  #[cfg(feature = "tikv")]
  pub async fn new_tikv_from_env() -> miette::Result<Self> {
    Ok(Self {
      inner: Arc::new(tikv_impl::TikvClient::new_from_env().await?),
    })
  }
  #[cfg(feature = "tikv")]
  /// Attempt with retry to create a new key-value store pointing to a TiKV
  pub async fn new_retryable_tikv_from_env(
    attempt_limit: u32,
    delay: std::time::Duration,
  ) -> Self {
    let kv_store_init =
      move || async move { tikv_impl::TikvClient::new_from_env().await };
    let retryable_tikv_store =
      hex::retryable::Retryable::init(attempt_limit, delay, kv_store_init)
        .await;
    Self {
      inner: Arc::new(retryable_tikv_store),
    }
  }
  /// Create a new mock store.
  #[cfg(feature = "mock")]
  pub fn new_mock() -> Self {
    {
      Self {
        inner: mock_impl::MockStore::new(),
      }
    }
  }
  /// Create a new store from a mock store.
  #[cfg(feature = "mock")]
  pub fn from_mock(mock: Arc<mock_impl::MockStore>) -> Self {
    Self { inner: mock }
  }
  /// Create a new key-value store built on a ReDB backend.
  #[cfg(feature = "redb")]
  pub fn new_redb(path: impl AsRef<std::path::Path>) -> miette::Result<Self> {
    Ok(Self {
      inner: Arc::new(redb_impl::RedbClient::new(path)?),
    })
  }

  /// Begin an optimistic transaction.
  pub async fn begin_optimistic_transaction(&self) -> KvResult<DynTransaction> {
    self.inner.begin_optimistic_transaction().await
  }

  /// Begin a pessimistic transaction.
  pub async fn begin_pessimistic_transaction(
    &self,
  ) -> KvResult<DynTransaction> {
    self.inner.begin_pessimistic_transaction().await
  }
}
