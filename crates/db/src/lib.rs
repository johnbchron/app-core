#![feature(iterator_try_collect)]

//! Provides access to the database.
//!
//! This is a hexagonal crate. It provides a [`Database`] struct.
//!
//! # [`Database`]
//! The [`Database`] struct provides a **tabular** database interface. It
//! provides CRUD operations for a generic item, using the
//! [`Model`](model::Model) trait to carry the table information.
//!
//! The implementation of the internal `DatabaseAdapter` trait is responsible
//! for organizing the database, and for bridging the gap between raw data in
//! the kv store and the model data.
//!
//! # Errors
//! Ideally, each method in [`Database`] should return a specific,
//! concrete error. This is the case for all but
//! [`enumerate_models`](Database::enumerate_models), which returns a
//! [`miette::Report`].

mod adapter;
mod kv_impl;
mod mock_impl;
#[cfg(test)]
mod tests;

use std::{fmt, sync::Arc};

use hex::health;
pub use kv;
use kv::EitherSlug;
use miette::Result;

pub use self::adapter::*;
use self::kv_impl::KvDatabaseAdapter;

/// A database.
#[derive(Clone)]
pub struct Database<M: model::Model> {
  inner: Arc<dyn DatabaseAdapter<M>>,
}

impl<M: model::Model + fmt::Debug> fmt::Debug for Database<M> {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct(stringify!(Database))
      .field("inner", &stringify!(Arc<dyn DatabaseAdapter<M>>))
      .finish()
  }
}

impl<M: model::Model> Database<M> {
  /// Creates a new database based on a key-value store.
  pub fn new_from_kv(kv_store: kv::KeyValueStore) -> Self {
    Self {
      inner: Arc::new(KvDatabaseAdapter::new(kv_store)),
    }
  }

  /// Creates a new database based on a mocked store.
  pub fn new_mock() -> Self {
    Self {
      inner: Arc::new(self::mock_impl::MockDatabaseAdapter::default()),
    }
  }

  /// Creates a new model.
  pub async fn create_model(&self, model: M) -> Result<M, CreateModelError> {
    self.inner.create_model(model).await
  }
  /// Fetches a model by its ID.
  pub async fn fetch_model_by_id(
    &self,
    id: model::RecordId<M>,
  ) -> Result<Option<M>, FetchModelError> {
    self.inner.fetch_model_by_id(id).await
  }
  /// Fetches a model by a unique index.
  ///
  /// Must be a valid index, defined in the model's
  /// [`UNIQUE_INDICES`](model::Model::UNIQUE_INDICES) constant.
  pub async fn fetch_model_by_unique_index(
    &self,
    index_name: String,
    index_value: EitherSlug,
  ) -> Result<Option<M>, FetchModelByIndexError> {
    self
      .inner
      .fetch_model_by_unique_index(index_name, index_value)
      .await
  }
  /// Fetches a model by an index.
  ///
  /// Must be a valid index, defined in the model's
  /// [`INDICES`](model::Model::INDICES) constant.
  pub async fn fetch_model_by_index(
    &self,
    index_name: String,
    index_value: EitherSlug,
  ) -> Result<Vec<M>, FetchModelByIndexError> {
    self
      .inner
      .fetch_models_by_index(index_name, index_value)
      .await
  }
  /// Produces a list of all model IDs.
  pub async fn enumerate_models(&self) -> Result<Vec<M>> {
    self.inner.enumerate_models().await
  }
  /// Updates an existing model with the provided changes.
  ///
  /// The model must exist in the database. This method will update the model
  /// and refresh any affected indices.
  pub async fn patch_model(
    &self,
    id: model::RecordId<M>,
    model: M,
  ) -> Result<M, PatchModelError> {
    self.inner.patch_model(id, model).await
  }
  /// Deletes a model by its ID.
  ///
  /// This will remove the model from the database and clean up all associated
  /// indices. Returns `true` if the model was deleted, `false` if it didn't
  /// exist.
  pub async fn delete_model(
    &self,
    id: model::RecordId<M>,
  ) -> Result<bool, DeleteModelError> {
    self.inner.delete_model(id).await
  }
}

#[async_trait::async_trait]
impl<M: model::Model> health::HealthReporter for Database<M> {
  fn name(&self) -> &'static str { stringify!(Database<M>) }
  async fn health_check(&self) -> health::ComponentHealth {
    health::AdditiveComponentHealth::from_futures(Some(
      self.inner.health_report(),
    ))
    .await
    .into()
  }
}
