//! Key-value store implementation.

mod consumptive;
mod keys;

use std::ops::Bound;

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
}

#[async_trait::async_trait]
impl<M: model::Model> DatabaseAdapter<M> for KvDatabaseAdapter {
  #[instrument(skip(self, model), fields(id = model.id().to_string(), table = M::TABLE_NAME))]
  async fn create_model(&self, model: M) -> Result<M, CreateModelError> {
    tracing::debug!("creating model");

    // the model itself will be stored under [model_name]:[id] -> model
    // and each index will be stored under
    // [model_name]_index_[index_name]:[index_value] -> [id]

    // calculate the key for the model
    let model_key = model_base_key::<M>(&model.id());
    let id_ulid: model::Ulid = model.id().into();

    // serialize the model into bytes
    let model_value = Value::serialize(&model)
      .into_diagnostic()
      .context("failed to serialize model")
      .map_err(CreateModelError::Serde)?;

    // serialize the id into bytes
    let id_value = Value::serialize(&id_ulid)
      .into_diagnostic()
      .context("failed to serialize id")
      .map_err(CreateModelError::Serde)?;

    // begin a transaction
    let txn = self
      .0
      .begin_pessimistic_transaction()
      .await
      .context("failed to begin pessimistic transaction")
      .map_err(CreateModelError::Db)?;

    // check if the model exists
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

    // insert the model
    let mut txn = txn
      .csm_insert(&model_key, model_value)
      .await
      .context("failed to insert model")
      .map_err(CreateModelError::Db)?;

    // insert the unique indexes
    for (u_index_name, u_index_fn) in M::UNIQUE_INDICES.iter() {
      let u_index_values = u_index_fn(&model);

      for u_index_value in u_index_values {
        // calculate the key for the index
        let u_index_key = unique_index_base_key::<M>(u_index_name)
          .with_either(u_index_value.clone());

        // check if the index exists already
        // reassignment is weird
        let (_txn, exists) = txn
          .csm_exists(&u_index_key)
          .await
          .context("failed to check if unique index exists")
          .map_err(CreateModelError::Db)?;
        txn = _txn;
        if exists {
          txn
            .to_rollback()
            .await
            .map_err(CreateModelError::RetryableTransaction)?;
          return Err(CreateModelError::UniqueIndexAlreadyExists {
            index_name:  u_index_name.to_string(),
            index_value: u_index_value,
          });
        }

        // insert the index
        txn = txn
          .csm_insert(&u_index_key, id_value.clone())
          .await
          .context("failed to insert unique index")
          .map_err(CreateModelError::Db)?;
      }
    }

    // insert the regular indexes
    for (index_name, index_fn) in M::INDICES.iter() {
      let index_values = index_fn(&model);

      for index_value in index_values {
        // calculate the key for the index
        // same as the unique index keys, but with the ID on the end
        let index_key = index_base_key::<M>(index_name)
          .with_either(index_value)
          .with(StrictSlug::new(id_ulid));

        // insert the index
        txn = txn
          .csm_insert(&index_key, id_value.clone())
          .await
          .context("failed to insert index")
          .map_err(CreateModelError::Db)?;
      }
    }

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
    // match model_value.map(|value| Value::deserialize::<M>(value)) {
    //   Some(Ok(v)) => Ok(),
    //   None => todo!(),
    // }
  }

  #[instrument(skip(self), fields(table = M::TABLE_NAME))]
  async fn fetch_model_by_unique_index(
    &self,
    index_name: String,
    index_value: EitherSlug,
  ) -> Result<Option<M>, FetchModelByIndexError> {
    tracing::debug!("fetching model by unique index");

    if !M::UNIQUE_INDICES
      .iter()
      .any(|(name, _)| name == &index_name)
    {
      return Err(FetchModelByIndexError::IndexDoesNotExistOnModel {
        index_name,
      });
    }

    let index_key =
      unique_index_base_key::<M>(&index_name).with_either(index_value.clone());

    let txn = self
      .0
      .begin_optimistic_transaction()
      .await
      .context("failed to begin optimistic transaction")
      .map_err(FetchModelByIndexError::RetryableTransaction)?;

    let (txn, id_value) = txn
      .csm_get(&index_key)
      .await
      .map_err(FetchModelByIndexError::Db)?;

    txn
      .to_commit()
      .await
      .map_err(FetchModelByIndexError::RetryableTransaction)?;

    let id = id_value
      .map(Value::deserialize::<model::RecordId<M>>)
      .transpose()
      .into_diagnostic()
      .context("failed to deserialize id")
      .map_err(FetchModelByIndexError::Serde)?;

    let id = match id {
      Some(id) => id,
      None => return Ok(None),
    };

    let model = match self
      .fetch_model_by_id(id)
      .await
      .map_err(FetchModelByIndexError::from)?
    {
      Some(model) => model,
      None => {
        return Err(FetchModelByIndexError::IndexMalformed {
          index_name,
          index_value,
        });
      }
    };

    Ok(Some(model))
  }

  #[instrument(skip(self), fields(table = M::TABLE_NAME))]
  async fn fetch_models_by_index(
    &self,
    index_name: String,
    index_value: EitherSlug,
  ) -> Result<Vec<M>, FetchModelByIndexError> {
    tracing::debug!("fetching model by index");

    let first_key = index_base_key::<M>(&index_name)
      .with_either(index_value.clone())
      .with(StrictSlug::new(model::RecordId::<M>::MIN().to_string()));
    let last_key = index_base_key::<M>(&index_name)
      .with_either(index_value)
      .with(StrictSlug::new(model::RecordId::<M>::MAX().to_string()));

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

    let ids = scan_results
      .into_iter()
      .map(|(_, value)| {
        Value::deserialize::<model::RecordId<M>>(value)
          .into_diagnostic()
          .context("failed to deserialize value into id")
          .map_err(FetchModelByIndexError::Serde)
      })
      .try_collect::<Vec<_>>()?;

    let mut model_values = Vec::with_capacity(ids.len());
    let mut txn = Some(txn);
    for id in ids {
      let model_key = model_base_key::<M>(&id);
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
  async fn enumerate_models(&self) -> Result<Vec<M>> {
    let first_key = model_base_key::<M>(&model::RecordId::<M>::MIN());
    let last_key = model_base_key::<M>(&model::RecordId::<M>::MAX());

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
    let id_ulid: model::Ulid = id.into();

    // serialize the updated model into bytes
    let model_value = Value::serialize(&model)
      .into_diagnostic()
      .context("failed to serialize model")
      .map_err(PatchModelError::Serde)?;

    // serialize the id into bytes
    let id_value = Value::serialize(&id_ulid)
      .into_diagnostic()
      .context("failed to serialize id")
      .map_err(PatchModelError::Serde)?;

    // begin a transaction
    let txn = self
      .0
      .begin_pessimistic_transaction()
      .await
      .context("failed to begin pessimistic transaction")
      .map_err(PatchModelError::Db)?;

    // fetch the existing model to compare indices
    let (txn, existing_model_value) = txn
      .csm_get(&model_key)
      .await
      .context("failed to fetch existing model")
      .map_err(PatchModelError::Db)?;

    let existing_model = match existing_model_value {
      Some(value) => Value::deserialize::<M>(value)
        .into_diagnostic()
        .context("failed to deserialize existing model")
        .map_err(PatchModelError::Serde)?,
      None => {
        txn
          .to_rollback()
          .await
          .map_err(PatchModelError::RetryableTransaction)?;
        return Err(PatchModelError::ModelNotFound);
      }
    };

    // update the model
    let mut txn = txn
      .csm_put(&model_key, model_value)
      .await
      .context("failed to update model")
      .map_err(PatchModelError::Db)?;

    // handle unique index updates
    for (u_index_name, u_index_fn) in M::UNIQUE_INDICES.iter() {
      let old_u_index_values = u_index_fn(&existing_model);
      let new_u_index_values = u_index_fn(&model);

      // remove old unique indices that are no longer present
      for old_value in &old_u_index_values {
        if !new_u_index_values.contains(old_value) {
          let old_u_index_key = unique_index_base_key::<M>(u_index_name)
            .with_either(old_value.clone());

          (txn, _) = txn
            .csm_delete(&old_u_index_key)
            .await
            .context("failed to delete old unique index")
            .map_err(PatchModelError::Db)?;
        }
      }

      // add new unique indices
      for new_value in &new_u_index_values {
        if !old_u_index_values.contains(new_value) {
          let new_u_index_key = unique_index_base_key::<M>(u_index_name)
            .with_either(new_value.clone());

          // check if the new index value already exists
          let (_txn, exists) = txn
            .csm_exists(&new_u_index_key)
            .await
            .context("failed to check if unique index exists")
            .map_err(PatchModelError::Db)?;
          txn = _txn;

          if exists {
            txn
              .to_rollback()
              .await
              .map_err(PatchModelError::RetryableTransaction)?;
            return Err(PatchModelError::UniqueIndexAlreadyExists {
              index_name:  u_index_name.to_string(),
              index_value: new_value.clone(),
            });
          }

          // insert the new unique index
          txn = txn
            .csm_insert(&new_u_index_key, id_value.clone())
            .await
            .context("failed to insert new unique index")
            .map_err(PatchModelError::Db)?;
        }
      }
    }

    // handle regular index updates
    for (index_name, index_fn) in M::INDICES.iter() {
      let old_index_values = index_fn(&existing_model);
      let new_index_values = index_fn(&model);

      // remove old indices that are no longer present
      for old_value in &old_index_values {
        if !new_index_values.contains(old_value) {
          let old_index_key = index_base_key::<M>(index_name)
            .with_either(old_value.clone())
            .with(StrictSlug::new(id_ulid));

          (txn, _) = txn
            .csm_delete(&old_index_key)
            .await
            .context("failed to delete old index")
            .map_err(PatchModelError::Db)?;
        }
      }

      // add new indices
      for new_value in &new_index_values {
        if !old_index_values.contains(new_value) {
          let new_index_key = index_base_key::<M>(index_name)
            .with_either(new_value.clone())
            .with(StrictSlug::new(id_ulid));

          txn = txn
            .csm_insert(&new_index_key, id_value.clone())
            .await
            .context("failed to insert new index")
            .map_err(PatchModelError::Db)?;
        }
      }
    }

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
    let id_ulid: model::Ulid = id.into();

    // begin a transaction
    let txn = self
      .0
      .begin_pessimistic_transaction()
      .await
      .context("failed to begin pessimistic transaction")
      .map_err(DeleteModelError::Db)?;

    // fetch the existing model to get index values for cleanup
    let (txn, existing_model_value) = txn
      .csm_get(&model_key)
      .await
      .context("failed to fetch existing model")
      .map_err(DeleteModelError::Db)?;

    let existing_model = match existing_model_value {
      Some(value) => Value::deserialize::<M>(value)
        .into_diagnostic()
        .context("failed to deserialize existing model")
        .map_err(DeleteModelError::Serde)?,
      None => {
        // model doesn't exist, nothing to delete
        txn
          .to_commit()
          .await
          .map_err(DeleteModelError::RetryableTransaction)?;
        return Ok(false);
      }
    };

    // delete the model
    let (mut txn, _) = txn
      .csm_delete(&model_key)
      .await
      .context("failed to delete model")
      .map_err(DeleteModelError::Db)?;

    // cleanup unique indices
    for (u_index_name, u_index_fn) in M::UNIQUE_INDICES.iter() {
      let u_index_values = u_index_fn(&existing_model);

      for u_index_value in u_index_values {
        let u_index_key =
          unique_index_base_key::<M>(u_index_name).with_either(u_index_value);

        (txn, _) = txn
          .csm_delete(&u_index_key)
          .await
          .context("failed to cleanup unique index")
          .map_err(DeleteModelError::FailedToCleanupIndices)?;
      }
    }

    // cleanup regular indices
    for (index_name, index_fn) in M::INDICES.iter() {
      let index_values = index_fn(&existing_model);

      for index_value in index_values {
        let index_key = index_base_key::<M>(index_name)
          .with_either(index_value)
          .with(StrictSlug::new(id_ulid));

        (txn, _) = txn
          .csm_delete(&index_key)
          .await
          .context("failed to cleanup index")
          .map_err(DeleteModelError::FailedToCleanupIndices)?;
      }
    }

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
