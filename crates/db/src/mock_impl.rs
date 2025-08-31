use std::{
  collections::{hash_map::Entry, HashMap},
  sync::Arc,
};

use hex::health;
use kv::*;
use model::{Model, RecordId};
use tokio::sync::Mutex;

use crate::{
  CreateModelError, DatabaseAdapter, DeleteModelError, FetchModelByIndexError,
  FetchModelError, PatchModelError,
};

#[derive(Clone, Debug)]
pub struct MockDatabaseAdapter<M>(Arc<MockDbInner<M>>);

impl<M> Default for MockDatabaseAdapter<M> {
  fn default() -> Self { MockDatabaseAdapter(Arc::new(MockDbInner::default())) }
}

#[allow(clippy::type_complexity)]
#[derive(Debug)]
struct MockDbInner<M> {
  models:    Mutex<HashMap<model::RecordId<M>, M>>,
  u_indices: Mutex<HashMap<String, HashMap<EitherSlug, model::RecordId<M>>>>,
  indices: Mutex<HashMap<String, HashMap<EitherSlug, Vec<model::RecordId<M>>>>>,
}

impl<M> Default for MockDbInner<M> {
  fn default() -> Self {
    Self {
      models:    Mutex::new(HashMap::new()),
      u_indices: Mutex::new(HashMap::new()),
      indices:   Mutex::new(HashMap::new()),
    }
  }
}

#[async_trait::async_trait]
impl<M: Send + Sync + 'static> health::HealthReporter
  for MockDatabaseAdapter<M>
{
  fn name(&self) -> &'static str { stringify!(MockDatabaseAdapter) }
  async fn health_check(&self) -> health::ComponentHealth {
    health::ComponentHealth::IntrensicallyUp
  }
}

#[async_trait::async_trait]
impl<M: Model> DatabaseAdapter<M> for MockDatabaseAdapter<M> {
  async fn create_model(&self, model: M) -> Result<M, CreateModelError> {
    let mut models = self.0.models.lock().await;
    match models.entry(model.id()) {
      Entry::Occupied(_) => return Err(CreateModelError::ModelAlreadyExists),
      Entry::Vacant(vacant_entry) => {
        vacant_entry.insert(model.clone());
      }
    }

    let mut u_indices = self.0.u_indices.lock().await;
    for (u_index_selector, u_index_getter) in M::UNIQUE_INDICES.iter() {
      let u_index = u_indices.entry(u_index_selector.to_string()).or_default();
      let u_index_values = u_index_getter(&model);

      for u_index_value in u_index_values {
        match u_index.entry(u_index_value.clone()) {
          Entry::Occupied(_) => {
            return Err(CreateModelError::UniqueIndexAlreadyExists {
              index_selector: u_index_selector.to_string(),
              index_value:    u_index_value,
            })
          }
          Entry::Vacant(vacant_entry) => {
            vacant_entry.insert(model.id());
          }
        }
      }
    }

    let mut indices = self.0.indices.lock().await;
    for (index_selector, index_getter) in M::INDICES.iter() {
      let index = indices.entry(index_selector.to_string()).or_default();
      let index_values = index_getter(&model);
      for index_value in index_values {
        let index_ids_for_value = index.entry(index_value).or_default();
        index_ids_for_value.push(model.id());
      }
    }

    Ok(model)
  }

  async fn fetch_model_by_id(
    &self,
    id: model::RecordId<M>,
  ) -> Result<Option<M>, FetchModelError> {
    Ok(self.0.models.lock().await.get(&id).cloned())
  }

  async fn fetch_model_by_unique_index(
    &self,
    index_selector: M::UniqueIndexSelector,
    index_value: EitherSlug,
  ) -> Result<Option<M>, FetchModelByIndexError> {
    let mut u_indices = self.0.u_indices.lock().await;
    let u_index = u_indices.entry(index_selector.to_string()).or_default();

    let id = u_index.get(&index_value);
    if let Some(id) = id {
      Ok(self.0.models.lock().await.get(id).cloned())
    } else {
      Ok(None)
    }
  }

  async fn fetch_ids_by_index(
    &self,
    index_selector: M::IndexSelector,
    index_value: EitherSlug,
  ) -> Result<Vec<RecordId<M>>, FetchModelByIndexError> {
    let mut indices = self.0.indices.lock().await;
    let index = indices.entry(index_selector.to_string()).or_default();

    let ids = index.get(&index_value).cloned().unwrap_or_default();

    Ok(ids)
  }

  async fn count_models_by_index(
    &self,
    index_selector: M::IndexSelector,
    index_value: EitherSlug,
  ) -> Result<u32, FetchModelByIndexError> {
    let mut indices = self.0.indices.lock().await;
    let index = indices.entry(index_selector.to_string()).or_default();

    let ids = index.get(&index_value).cloned().unwrap_or_default();
    Ok(ids.len() as _)
  }

  async fn enumerate_models(&self) -> miette::Result<Vec<M>> {
    Ok(self.0.models.lock().await.values().cloned().collect())
  }

  async fn patch_model(
    &self,
    id: model::RecordId<M>,
    model: M,
  ) -> Result<M, PatchModelError> {
    // First check if the model exists
    {
      let models = self.0.models.lock().await;
      if !models.contains_key(&id) {
        return Err(PatchModelError::ModelNotFound);
      }
    }

    // Check unique index constraints for the new model
    let mut u_indices = self.0.u_indices.lock().await;
    for (u_index_selector, u_index_getter) in M::UNIQUE_INDICES.iter() {
      let u_index = u_indices.entry(u_index_selector.to_string()).or_default();
      let u_index_values = u_index_getter(&model);

      for u_index_value in u_index_values {
        match u_index.entry(u_index_value.clone()) {
          Entry::Occupied(occupied_entry) => {
            // If the existing entry points to a different model, it's a
            // constraint violation
            if *occupied_entry.get() != id {
              return Err(PatchModelError::UniqueIndexAlreadyExists {
                index_selector: u_index_selector.to_string(),
                index_value:    u_index_value,
              });
            }
            // If it points to the same model, we're updating with the same
            // unique value, which is fine
          }
          Entry::Vacant(_) => {
            // This unique index value doesn't exist yet, so we'll need to add
            // it
          }
        }
      }
    }

    // Get the old model to clean up its indices
    let old_model = {
      let models = self.0.models.lock().await;
      models.get(&id).cloned().unwrap() // We already checked it exists
    };

    // Remove old unique indices
    for (u_index_selector, u_index_getter) in M::UNIQUE_INDICES.iter() {
      let u_index = u_indices.entry(u_index_selector.to_string()).or_default();
      let old_u_index_values = u_index_getter(&old_model);
      for old_u_index_value in old_u_index_values {
        u_index.remove(&old_u_index_value);
      }
    }

    // Add new unique indices
    for (u_index_selector, u_index_getter) in M::UNIQUE_INDICES.iter() {
      let u_index = u_indices.entry(u_index_selector.to_string()).or_default();
      let new_u_index_values = u_index_getter(&model);
      for new_u_index_value in new_u_index_values {
        u_index.insert(new_u_index_value, id);
      }
    }

    // Update regular indices
    let mut indices = self.0.indices.lock().await;

    // Remove old indices
    for (index_selector, index_getter) in M::INDICES.iter() {
      let index = indices.entry(index_selector.to_string()).or_default();
      let old_index_values = index_getter(&old_model);
      for old_index_value in old_index_values {
        if let Some(index_ids) = index.get_mut(&old_index_value) {
          index_ids.retain(|&existing_id| existing_id != id);
          if index_ids.is_empty() {
            index.remove(&old_index_value);
          }
        }
      }
    }

    // Add new indices
    for (index_selector, index_getter) in M::INDICES.iter() {
      let index = indices.entry(index_selector.to_string()).or_default();
      let new_index_values = index_getter(&model);
      for new_index_value in new_index_values {
        let index_ids_for_value = index.entry(new_index_value).or_default();
        index_ids_for_value.push(id);
      }
    }

    // Update the model
    {
      let mut models = self.0.models.lock().await;
      models.insert(id, model.clone());
    }

    Ok(model)
  }

  async fn delete_model(
    &self,
    id: model::RecordId<M>,
  ) -> Result<bool, DeleteModelError> {
    // Get the model to delete (if it exists)
    let model_to_delete = {
      let models = self.0.models.lock().await;
      models.get(&id).cloned()
    };

    let Some(model_to_delete) = model_to_delete else {
      // Model doesn't exist, return false
      return Ok(false);
    };

    // Remove from models
    {
      let mut models = self.0.models.lock().await;
      models.remove(&id);
    }

    // Clean up unique indices
    {
      let mut u_indices = self.0.u_indices.lock().await;
      for (u_index_selector, u_index_getter) in M::UNIQUE_INDICES.iter() {
        let u_index =
          u_indices.entry(u_index_selector.to_string()).or_default();
        let u_index_values = u_index_getter(&model_to_delete);
        for u_index_value in u_index_values {
          u_index.remove(&u_index_value);
        }
      }
    }

    // Clean up regular indices
    {
      let mut indices = self.0.indices.lock().await;
      for (index_selector, index_getter) in M::INDICES.iter() {
        let index = indices.entry(index_selector.to_string()).or_default();
        let index_values = index_getter(&model_to_delete);
        for index_value in index_values {
          if let Some(index_ids) = index.get_mut(&index_value) {
            index_ids.retain(|&existing_id| existing_id != id);
            if index_ids.is_empty() {
              index.remove(&index_value);
            }
          }
        }
      }
    }

    Ok(true)
  }
}
