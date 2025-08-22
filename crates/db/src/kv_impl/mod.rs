//! Key-value store implementation.

mod consumptive;
mod keys;

use std::{collections::HashSet, ops::Bound};

use hex::health::{self, HealthAware};
use kv::*;
use miette::{Context, IntoDiagnostic, Result};
use tracing::instrument;

use self::{consumptive::ConsumptiveTransaction, keys::*};
use crate::{
  adapter::{FetchModelByIndexError, FetchModelError},
  CreateModelError, DatabaseAdapter, DeleteModelError, PatchModelError,
};

/// A [`KeyValueStore`]-based database adapter.
#[derive(Clone)]
pub struct KvDatabaseAdapter(KeyValueStore);

impl KvDatabaseAdapter {
  /// Creates a new [`KeyValueStore`] adapter.
  pub fn new(kv_store: KeyValueStore) -> Self { Self(kv_store) }

  /// Helper to serialize model and id values
  fn serialize_model_and_id<M: model::Model>(
    &self,
    model: &M,
  ) -> Result<(Value, Value), CreateModelError> {
    let model_value = Value::serialize(model)
      .into_diagnostic()
      .context("failed to serialize model")
      .map_err(CreateModelError::Serde)?;

    let id_ulid: model::Ulid = model.id().into();
    let id_value = Value::serialize(&id_ulid)
      .into_diagnostic()
      .context("failed to serialize id")
      .map_err(CreateModelError::Serde)?;

    Ok((model_value, id_value))
  }

  /// Helper to get or create ID set for an index
  async fn get_id_set_for_index<T: ConsumptiveTransaction>(
    &self,
    txn: T,
    index_key: &Key,
  ) -> Result<(T, HashSet<model::Ulid>), miette::Report> {
    let (txn, existing_value) = txn.csm_get(index_key).await?;

    let id_set = if let Some(value) = existing_value {
      Value::deserialize::<HashSet<model::Ulid>>(value)
        .into_diagnostic()
        .context("failed to deserialize ID set")?
    } else {
      HashSet::new()
    };

    Ok((txn, id_set))
  }

  /// Helper to store ID set for an index
  async fn store_id_set_for_index<T: ConsumptiveTransaction>(
    &self,
    txn: T,
    index_key: &Key,
    id_set: HashSet<model::Ulid>,
  ) -> Result<T, miette::Report> {
    if id_set.is_empty() {
      // If the set is empty, delete the key entirely
      let (txn, _) = txn.csm_delete(index_key).await?;
      Ok(txn)
    } else {
      let id_set_value = Value::serialize(&id_set)
        .into_diagnostic()
        .context("failed to serialize ID set")?;
      let txn = txn.csm_put(index_key, id_set_value).await?;
      Ok(txn)
    }
  }

  /// Helper to handle unique index operations during create/patch
  async fn handle_unique_indices<M: model::Model, T: ConsumptiveTransaction>(
    &self,
    txn: T,
    model: &M,
    operation: IndexOperation,
  ) -> Result<T, CreateModelError> {
    let mut current_txn = txn;
    let id_ulid: model::Ulid = model.id().into();

    for (u_index_selector, u_index_fn) in M::UNIQUE_INDICES.iter() {
      let u_index_values = u_index_fn(model);

      for u_index_value in u_index_values {
        let u_index_key = unique_index_base_key::<M>(*u_index_selector)
          .with_either(u_index_value.clone());

        current_txn = match operation {
          IndexOperation::Insert => {
            // Get existing ID set
            let (txn, mut id_set) = self
              .get_id_set_for_index(current_txn, &u_index_key)
              .await
              .context("failed to get ID set for unique index")
              .map_err(CreateModelError::Db)?;
            current_txn = txn;

            // For unique indices, the set should be empty or contain only this
            // ID
            if !id_set.is_empty() && !id_set.contains(&id_ulid) {
              current_txn
                .to_rollback()
                .await
                .map_err(CreateModelError::RetryableTransaction)?;
              return Err(CreateModelError::UniqueIndexAlreadyExists {
                index_selector: u_index_selector.to_string(),
                index_value:    u_index_value,
              });
            }

            // Add the ID to the set
            id_set.insert(id_ulid);

            // Store the updated set
            current_txn = self
              .store_id_set_for_index(current_txn, &u_index_key, id_set)
              .await
              .context("failed to store ID set for unique index")
              .map_err(CreateModelError::Db)?;

            current_txn
          }
          IndexOperation::Delete => {
            // Get existing ID set
            let (txn, mut id_set) = self
              .get_id_set_for_index(current_txn, &u_index_key)
              .await
              .context("failed to get ID set for unique index")
              .map_err(CreateModelError::Db)?;
            current_txn = txn;

            // Remove the ID from the set
            id_set.remove(&id_ulid);

            // Store the updated set (or delete if empty)
            current_txn = self
              .store_id_set_for_index(current_txn, &u_index_key, id_set)
              .await
              .context("failed to store ID set for unique index")
              .map_err(CreateModelError::Db)?;

            current_txn
          }
        };
      }
    }

    Ok(current_txn)
  }

  /// Helper to handle regular index operations during create/patch
  async fn handle_regular_indices<
    M: model::Model,
    T: ConsumptiveTransaction,
  >(
    &self,
    txn: T,
    model: &M,
    operation: IndexOperation,
  ) -> Result<T, CreateModelError> {
    let mut current_txn = txn;
    let id_ulid: model::Ulid = model.id().into();

    for (index_selector, index_fn) in M::INDICES.iter() {
      let index_values = index_fn(model);

      for index_value in index_values {
        let index_key =
          index_base_key::<M>(*index_selector).with_either(index_value);

        current_txn = match operation {
          IndexOperation::Insert => {
            // Get existing ID set
            let (txn, mut id_set) = self
              .get_id_set_for_index(current_txn, &index_key)
              .await
              .context("failed to get ID set for index")
              .map_err(CreateModelError::Db)?;
            current_txn = txn;

            // Add the ID to the set
            id_set.insert(id_ulid);

            // Store the updated set
            current_txn = self
              .store_id_set_for_index(current_txn, &index_key, id_set)
              .await
              .context("failed to store ID set for index")
              .map_err(CreateModelError::Db)?;

            current_txn
          }
          IndexOperation::Delete => {
            // Get existing ID set
            let (txn, mut id_set) = self
              .get_id_set_for_index(current_txn, &index_key)
              .await
              .context("failed to get ID set for index")
              .map_err(CreateModelError::Db)?;
            current_txn = txn;

            // Remove the ID from the set
            id_set.remove(&id_ulid);

            // Store the updated set (or delete if empty)
            current_txn = self
              .store_id_set_for_index(current_txn, &index_key, id_set)
              .await
              .context("failed to store ID set for index")
              .map_err(CreateModelError::Db)?;

            current_txn
          }
        };
      }
    }

    Ok(current_txn)
  }

  /// Helper to handle unique index updates during patch operations
  async fn handle_unique_index_updates<
    M: model::Model,
    T: ConsumptiveTransaction,
  >(
    &self,
    txn: T,
    existing_model: &M,
    new_model: &M,
  ) -> Result<T, PatchModelError> {
    let mut current_txn = txn;
    let id_ulid: model::Ulid = new_model.id().into();

    for (u_index_selector, u_index_fn) in M::UNIQUE_INDICES.iter() {
      let old_u_index_values = u_index_fn(existing_model);
      let new_u_index_values = u_index_fn(new_model);

      // Remove from old unique indices that are no longer present
      for old_value in &old_u_index_values {
        if !new_u_index_values.contains(old_value) {
          let old_u_index_key = unique_index_base_key::<M>(*u_index_selector)
            .with_either(old_value.clone());

          // Get existing ID set
          let (txn, mut id_set) = self
            .get_id_set_for_index(current_txn, &old_u_index_key)
            .await
            .context("failed to get ID set for old unique index")
            .map_err(PatchModelError::Db)?;
          current_txn = txn;

          // Remove the ID from the set
          id_set.remove(&id_ulid);

          // Store the updated set (or delete if empty)
          current_txn = self
            .store_id_set_for_index(current_txn, &old_u_index_key, id_set)
            .await
            .context("failed to store ID set for old unique index")
            .map_err(PatchModelError::Db)?;
        }
      }

      // Add to new unique indices
      for new_value in &new_u_index_values {
        if !old_u_index_values.contains(new_value) {
          let new_u_index_key = unique_index_base_key::<M>(*u_index_selector)
            .with_either(new_value.clone());

          // Get existing ID set
          let (txn, mut id_set) = self
            .get_id_set_for_index(current_txn, &new_u_index_key)
            .await
            .context("failed to get ID set for new unique index")
            .map_err(PatchModelError::Db)?;
          current_txn = txn;

          // For unique indices, the set should be empty or contain only this ID
          if !id_set.is_empty() && !id_set.contains(&id_ulid) {
            current_txn
              .to_rollback()
              .await
              .map_err(PatchModelError::RetryableTransaction)?;
            return Err(PatchModelError::UniqueIndexAlreadyExists {
              index_selector: u_index_selector.to_string(),
              index_value:    new_value.clone(),
            });
          }

          // Add the ID to the set
          id_set.insert(id_ulid);

          // Store the updated set
          current_txn = self
            .store_id_set_for_index(current_txn, &new_u_index_key, id_set)
            .await
            .context("failed to store ID set for new unique index")
            .map_err(PatchModelError::Db)?;
        }
      }
    }

    Ok(current_txn)
  }

  /// Helper to handle regular index updates during patch operations
  async fn handle_regular_index_updates<
    M: model::Model,
    T: ConsumptiveTransaction,
  >(
    &self,
    txn: T,
    existing_model: &M,
    new_model: &M,
  ) -> Result<T, PatchModelError> {
    let mut current_txn = txn;
    let id_ulid: model::Ulid = new_model.id().into();

    for (index_selector, index_fn) in M::INDICES.iter() {
      let old_index_values = index_fn(existing_model);
      let new_index_values = index_fn(new_model);

      // Remove from old indices that are no longer present
      for old_value in &old_index_values {
        if !new_index_values.contains(old_value) {
          let old_index_key =
            index_base_key::<M>(*index_selector).with_either(old_value.clone());

          // Get existing ID set
          let (txn, mut id_set) = self
            .get_id_set_for_index(current_txn, &old_index_key)
            .await
            .context("failed to get ID set for old index")
            .map_err(PatchModelError::Db)?;
          current_txn = txn;

          // Remove the ID from the set
          id_set.remove(&id_ulid);

          // Store the updated set (or delete if empty)
          current_txn = self
            .store_id_set_for_index(current_txn, &old_index_key, id_set)
            .await
            .context("failed to store ID set for old index")
            .map_err(PatchModelError::Db)?;
        }
      }

      // Add to new indices
      for new_value in &new_index_values {
        if !old_index_values.contains(new_value) {
          let new_index_key =
            index_base_key::<M>(*index_selector).with_either(new_value.clone());

          // Get existing ID set
          let (txn, mut id_set) = self
            .get_id_set_for_index(current_txn, &new_index_key)
            .await
            .context("failed to get ID set for new index")
            .map_err(PatchModelError::Db)?;
          current_txn = txn;

          // Add the ID to the set
          id_set.insert(id_ulid);

          // Store the updated set
          current_txn = self
            .store_id_set_for_index(current_txn, &new_index_key, id_set)
            .await
            .context("failed to store ID set for new index")
            .map_err(PatchModelError::Db)?;
        }
      }
    }

    Ok(current_txn)
  }

  /// Helper to get model key range for scanning
  fn get_model_key_range<M: model::Model>() -> (Key, Key) {
    let first_key = model_base_key::<M>(&model::RecordId::<M>::MIN());
    let last_key = model_base_key::<M>(&model::RecordId::<M>::MAX());
    (first_key, last_key)
  }

  /// Helper to fetch existing model during patch/delete operations
  async fn fetch_existing_model<M: model::Model, T: ConsumptiveTransaction>(
    &self,
    txn: T,
    id: &model::RecordId<M>,
  ) -> Result<(T, Option<M>), miette::Report> {
    let model_key = model_base_key::<M>(id);

    let (txn, existing_model_value) = txn
      .csm_get(&model_key)
      .await
      .context("failed to fetch existing model")?;

    let existing_model = existing_model_value
      .map(|value| {
        Value::deserialize::<M>(value)
          .into_diagnostic()
          .context("failed to deserialize existing model")
      })
      .transpose()?;

    Ok((txn, existing_model))
  }
}

#[derive(Copy, Clone)]
enum IndexOperation {
  Insert,
  Delete,
}

#[async_trait::async_trait]
impl<M: model::Model> DatabaseAdapter<M> for KvDatabaseAdapter {
  #[instrument(skip(self, model), fields(id = model.id().to_string(), table = M::TABLE_NAME))]
  async fn create_model(&self, model: M) -> Result<M, CreateModelError> {
    tracing::debug!("creating model");

    let model_key = model_base_key::<M>(&model.id());
    let (model_value, _id_value) = self.serialize_model_and_id(&model)?;

    let txn = self
      .0
      .begin_pessimistic_transaction()
      .await
      .context("failed to begin pessimistic transaction")
      .map_err(CreateModelError::Db)?;

    // Check if the model exists
    let (txn, exists) = txn
      .csm_exists(&model_key)
      .await
      .context("failed to check if model exists")
      .map_err(CreateModelError::Db)?;
    if exists {
      txn
        .to_rollback()
        .await
        .map_err(CreateModelError::RetryableTransaction)?;
      return Err(CreateModelError::ModelAlreadyExists);
    }

    // Insert the model
    let txn = txn
      .csm_insert(&model_key, model_value)
      .await
      .context("failed to insert model")
      .map_err(CreateModelError::Db)?;

    // Handle unique indices
    let txn = self
      .handle_unique_indices(txn, &model, IndexOperation::Insert)
      .await?;

    // Handle regular indices
    let txn = self
      .handle_regular_indices(txn, &model, IndexOperation::Insert)
      .await?;

    txn
      .to_commit()
      .await
      .map_err(CreateModelError::RetryableTransaction)?;

    Ok(model)
  }

  #[instrument(skip(self), fields(table = M::TABLE_NAME))]
  async fn fetch_model_by_id(
    &self,
    id: model::RecordId<M>,
  ) -> Result<Option<M>, FetchModelError> {
    tracing::debug!("fetching model with id");

    let model_key = model_base_key::<M>(&id);

    let txn = self
      .0
      .begin_optimistic_transaction()
      .await
      .context("failed to begin optimistic transaction")
      .map_err(FetchModelError::RetryableTransaction)?;

    let (txn, model_value) =
      txn.csm_get(&model_key).await.map_err(FetchModelError::Db)?;

    txn
      .to_commit()
      .await
      .map_err(FetchModelError::RetryableTransaction)?;

    model_value
      .map(|value| Value::deserialize(value))
      .transpose()
      .into_diagnostic()
      .context("failed to deserialize model")
      .map_err(FetchModelError::Serde)
  }

  #[instrument(skip(self), fields(table = M::TABLE_NAME))]
  async fn fetch_model_by_unique_index(
    &self,
    index_selector: M::UniqueIndexSelector,
    index_value: EitherSlug,
  ) -> Result<Option<M>, FetchModelByIndexError> {
    tracing::debug!("fetching model by unique index");

    let index_key = unique_index_base_key::<M>(index_selector)
      .with_either(index_value.clone());

    let txn = self
      .0
      .begin_optimistic_transaction()
      .await
      .context("failed to begin optimistic transaction")
      .map_err(FetchModelByIndexError::RetryableTransaction)?;

    let (txn, id_set_value) = txn
      .csm_get(&index_key)
      .await
      .map_err(FetchModelByIndexError::Db)?;

    txn
      .to_commit()
      .await
      .map_err(FetchModelByIndexError::RetryableTransaction)?;

    let id_set = id_set_value
      .map(Value::deserialize::<HashSet<model::Ulid>>)
      .transpose()
      .into_diagnostic()
      .context("failed to deserialize ID set")
      .map_err(FetchModelByIndexError::Serde)?;

    let id_set = match id_set {
      Some(set) => set,
      None => {
        return Ok(None);
      }
    };

    // For unique indices, there should be exactly one ID
    if id_set.len() != 1 {
      return Err(FetchModelByIndexError::IndexMalformed {
        index_selector: index_selector.to_string(),
        index_value,
      });
    }

    let id = id_set.into_iter().next().unwrap();
    let record_id = model::RecordId::<M>::from_ulid(id);

    let model = match self
      .fetch_model_by_id(record_id)
      .await
      .map_err(FetchModelByIndexError::from)?
    {
      Some(model) => model,
      None => {
        return Err(FetchModelByIndexError::IndexMalformed {
          index_selector: index_selector.to_string(),
          index_value,
        });
      }
    };

    Ok(Some(model))
  }

  #[instrument(skip(self), fields(table = M::TABLE_NAME))]
  async fn fetch_models_by_index(
    &self,
    index_selector: M::IndexSelector,
    index_value: EitherSlug,
  ) -> Result<Vec<M>, FetchModelByIndexError> {
    tracing::debug!("fetching model by index");

    let index_key =
      index_base_key::<M>(index_selector).with_either(index_value);

    let txn = self
      .0
      .begin_optimistic_transaction()
      .await
      .context("failed to begin optimistic transaction")
      .map_err(FetchModelError::RetryableTransaction)?;

    let (txn, id_set_value) =
      txn.csm_get(&index_key).await.map_err(FetchModelError::Db)?;

    let id_set = id_set_value
      .map(Value::deserialize::<HashSet<model::Ulid>>)
      .transpose()
      .into_diagnostic()
      .context("failed to deserialize ID set")
      .map_err(FetchModelByIndexError::Serde)?;

    let id_set = match id_set {
      Some(set) => set,
      None => {
        txn
          .to_commit()
          .await
          .map_err(FetchModelByIndexError::RetryableTransaction)?;
        return Ok(Vec::new());
      }
    };

    // Fetch all models for the IDs in the set
    let mut model_values = Vec::with_capacity(id_set.len());
    let mut txn = Some(txn);

    for id_ulid in id_set {
      let record_id = model::RecordId::<M>::from_ulid(id_ulid);
      let model_key = model_base_key::<M>(&record_id);
      let (_txn, model_value) = txn
        .take()
        .expect("txn wasn't put back in the option, for some reason")
        .csm_get(&model_key)
        .await
        .map_err(FetchModelError::Db)?;
      txn = Some(_txn);
      model_values.push(model_value);
    }

    txn
      .expect("txn wasn't put back in the option, for some reason")
      .to_commit()
      .await
      .map_err(FetchModelByIndexError::RetryableTransaction)?;

    let models = model_values
      .into_iter()
      .flatten()
      .map(|value| {
        Value::deserialize::<M>(value)
          .into_diagnostic()
          .context("failed to deserialize value into model")
          .map_err(FetchModelByIndexError::Serde)
      })
      .try_collect::<Vec<_>>()?;

    Ok(models)
  }

  #[instrument(skip(self), fields(table = M::TABLE_NAME))]
  async fn count_models_by_index(
    &self,
    index_selector: M::IndexSelector,
    index_value: EitherSlug,
  ) -> Result<u32, FetchModelByIndexError> {
    tracing::debug!("counting models by index");

    let index_key =
      index_base_key::<M>(index_selector).with_either(index_value);

    let txn = self
      .0
      .begin_optimistic_transaction()
      .await
      .context("failed to begin optimistic transaction")
      .map_err(FetchModelByIndexError::RetryableTransaction)?;

    let (txn, id_set_value) = txn
      .csm_get(&index_key)
      .await
      .map_err(FetchModelByIndexError::Db)?;

    txn
      .to_commit()
      .await
      .map_err(FetchModelByIndexError::RetryableTransaction)?;

    let id_set = id_set_value
      .map(Value::deserialize::<HashSet<model::Ulid>>)
      .transpose()
      .into_diagnostic()
      .context("failed to deserialize ID set")
      .map_err(FetchModelByIndexError::Serde)?;

    Ok(id_set.map(|set| set.len() as u32).unwrap_or(0))
  }

  #[instrument(skip(self), fields(table = M::TABLE_NAME))]
  async fn enumerate_models(&self) -> Result<Vec<M>> {
    let (first_key, last_key) = Self::get_model_key_range::<M>();

    let txn = self
      .0
      .begin_optimistic_transaction()
      .await
      .context("failed to begin optimistic transaction")
      .map_err(FetchModelError::RetryableTransaction)?;

    let (txn, scan_results) = txn
      .csm_scan(Bound::Included(first_key), Bound::Included(last_key), None)
      .await
      .map_err(FetchModelError::Db)?;

    txn
      .to_commit()
      .await
      .map_err(FetchModelError::RetryableTransaction)?;

    let models = scan_results
      .into_iter()
      .map(|(_, value)| {
        Value::deserialize::<M>(value)
          .into_diagnostic()
          .context("failed to deserialize value into model")
          .map_err(FetchModelError::Serde)
          .map_err(miette::Report::from)
      })
      .collect::<Result<Vec<M>>>()?;

    Ok(models)
  }

  #[instrument(skip(self, model), fields(id = id.to_string(), table = M::TABLE_NAME))]
  async fn patch_model(
    &self,
    id: model::RecordId<M>,
    model: M,
  ) -> Result<M, PatchModelError> {
    tracing::debug!("patching model");

    let model_key = model_base_key::<M>(&id);
    let (model_value, _id_value) =
      self.serialize_model_and_id(&model).map_err(|e| match e {
        CreateModelError::Serde(s) => PatchModelError::Serde(s),
        _ => unreachable!("serialize_model_and_id only returns Serde errors"),
      })?;

    let txn = self
      .0
      .begin_pessimistic_transaction()
      .await
      .context("failed to begin pessimistic transaction")
      .map_err(PatchModelError::Db)?;

    // Fetch the existing model
    let (txn, existing_model) = self
      .fetch_existing_model(txn, &id)
      .await
      .map_err(PatchModelError::Db)?;

    let existing_model = match existing_model {
      Some(model) => model,
      None => {
        txn
          .to_rollback()
          .await
          .map_err(PatchModelError::RetryableTransaction)?;
        return Err(PatchModelError::ModelNotFound);
      }
    };

    // Update the model
    let txn = txn
      .csm_put(&model_key, model_value)
      .await
      .context("failed to update model")
      .map_err(PatchModelError::Db)?;

    // Handle unique index updates
    let txn = self
      .handle_unique_index_updates(txn, &existing_model, &model)
      .await?;

    // Handle regular index updates
    let txn = self
      .handle_regular_index_updates(txn, &existing_model, &model)
      .await?;

    txn
      .to_commit()
      .await
      .map_err(PatchModelError::RetryableTransaction)?;

    Ok(model)
  }

  #[instrument(skip(self), fields(id = id.to_string(), table = M::TABLE_NAME))]
  async fn delete_model(
    &self,
    id: model::RecordId<M>,
  ) -> Result<bool, DeleteModelError> {
    tracing::debug!("deleting model");

    let model_key = model_base_key::<M>(&id);

    let txn = self
      .0
      .begin_pessimistic_transaction()
      .await
      .context("failed to begin pessimistic transaction")
      .map_err(DeleteModelError::Db)?;

    // Fetch the existing model
    let (txn, existing_model) = self
      .fetch_existing_model(txn, &id)
      .await
      .map_err(DeleteModelError::Db)?;

    let existing_model = match existing_model {
      Some(model) => model,
      None => {
        // Model doesn't exist, nothing to delete
        txn
          .to_commit()
          .await
          .map_err(DeleteModelError::RetryableTransaction)?;
        return Ok(false);
      }
    };

    // Delete the model
    let (txn, _) = txn
      .csm_delete(&model_key)
      .await
      .context("failed to delete model")
      .map_err(DeleteModelError::Db)?;

    // Cleanup indices using the existing model data
    // Use the helper methods for cleanup (converting errors appropriately)
    let txn = self
      .handle_unique_indices(txn, &existing_model, IndexOperation::Delete)
      .await
      .map_err(|e| match e {
        CreateModelError::Db(db_err) => {
          DeleteModelError::FailedToCleanupIndices(db_err)
        }
        _ => unreachable!(
          "handle_unique_indices with Delete should only return Db errors"
        ),
      })?;

    let txn = self
      .handle_regular_indices(txn, &existing_model, IndexOperation::Delete)
      .await
      .map_err(|e| match e {
        CreateModelError::Db(db_err) => {
          DeleteModelError::FailedToCleanupIndices(db_err)
        }
        _ => unreachable!(
          "handle_regular_indices with Delete should only return Db errors"
        ),
      })?;

    txn
      .to_commit()
      .await
      .map_err(DeleteModelError::RetryableTransaction)?;

    Ok(true)
  }
}

#[async_trait::async_trait]
impl health::HealthReporter for KvDatabaseAdapter {
  fn name(&self) -> &'static str { stringify!(KvDatabaseAdapter) }
  async fn health_check(&self) -> health::ComponentHealth {
    health::AdditiveComponentHealth::from_futures(Some(self.0.health_report()))
      .await
      .into()
  }
}
