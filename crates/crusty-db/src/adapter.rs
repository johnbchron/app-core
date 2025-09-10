mod errors;

use hex::Hexagonal;
use kv::*;
use miette::Result;
use model::RecordId;

pub use self::errors::*;

/// An adapter for a model-based database.
#[async_trait::async_trait]
pub(crate) trait DatabaseAdapter<M: model::Model>: Hexagonal {
  /// Creates a new model.
  async fn create_model(&self, model: M) -> Result<M, CreateModelError>;

  /// Fetches a model by its ID.
  async fn fetch_model_by_id(
    &self,
    id: model::RecordId<M>,
  ) -> Result<Option<M>, FetchModelError>;

  /// Fetches a model by an index.
  ///
  /// Must be a valid index, defined in the model's
  /// [`UNIQUE_INDICES`](model::Model::UNIQUE_INDICES) constant.
  async fn fetch_model_by_unique_index(
    &self,
    index_selector: M::UniqueIndexSelector,
    index_value: EitherSlug,
  ) -> Result<Option<M>, FetchModelByIndexError>;

  /// Fetches the IDs of models that match the index value.
  ///
  /// Must be a valid index, defined in the model's
  /// [`INDICES`](model::Model::INDICES) constant.
  async fn fetch_ids_by_index(
    &self,
    index_selector: M::IndexSelector,
    index_value: EitherSlug,
  ) -> Result<Vec<RecordId<M>>, FetchModelByIndexError>;

  /// Counts the models that match the index value.
  ///
  /// Must be a valid index, defined in the model's
  /// [`INDICES`](model::Model::INDICES) constant.
  async fn count_models_by_index(
    &self,
    index_selector: M::IndexSelector,
    index_value: EitherSlug,
  ) -> Result<u32, FetchModelByIndexError>;

  /// Produces a list of all model IDs.
  async fn enumerate_models(&self) -> Result<Vec<M>>;

  /// Updates an existing model with the provided changes.
  ///
  /// The model must exist in the database. This method will update the model
  /// and refresh any affected indices.
  async fn patch_model(
    &self,
    id: model::RecordId<M>,
    model: M,
  ) -> Result<M, PatchModelError>;

  /// Deletes a model by its ID.
  ///
  /// This will remove the model from the database and clean up all associated
  /// indices. Returns `true` if the model was deleted, `false` if it didn't
  /// exist.
  async fn delete_model(
    &self,
    id: model::RecordId<M>,
  ) -> Result<bool, DeleteModelError>;
}
