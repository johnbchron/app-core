use kv::*;
use model::Model;
use serde::{Deserialize, Serialize};
use ulid::Ulid;

use crate::Database;

type TestModelRecordId = model::RecordId<TestModel>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
struct TestModel {
  id:    TestModelRecordId,
  name:  StrictSlug,
  owner: Ulid,
}

impl Model for TestModel {
  const TABLE_NAME: &'static str = "test_model";
  const UNIQUE_INDICES: &'static [(
    &'static str,
    model::SlugFieldGetter<Self>,
  )] = &[("name", move |m| vec![EitherSlug::Strict(m.name.clone())])];
  fn id(&self) -> TestModelRecordId { self.id }
  const INDICES: &'static [(&'static str, model::SlugFieldGetter<Self>)] =
    &[("owner", move |m| {
      vec![EitherSlug::Strict(StrictSlug::new(m.owner.to_string()))]
    })];
}

trait DbInstantiator {
  fn init<M: Model>() -> Database<M>;
}

struct MockInstantiator;

impl DbInstantiator for MockInstantiator {
  fn init<M: Model>() -> Database<M> { Database::new_mock() }
}

struct KvMockedInstantiator;

impl DbInstantiator for KvMockedInstantiator {
  fn init<M: Model>() -> Database<M> {
    Database::new_from_kv(KeyValueStore::new_mock())
  }
}

#[generic_tests::define(attrs(tokio::test))]
mod generic_testing {
  use kv::*;
  use model::Model;
  use ulid::Ulid;

  use super::{
    DbInstantiator, KvMockedInstantiator, MockInstantiator, TestModel,
  };
  use crate::{CreateModelError, FetchModelByIndexError};

  #[tokio::test]
  async fn test_create_model<I: DbInstantiator>() {
    let db = I::init();

    let model = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test"),
      owner: Ulid::new(),
    };

    let created_model = db.create_model(model.clone()).await.unwrap();
    assert_eq!(model, created_model);

    let fetched_model =
      db.fetch_model_by_id(model.id()).await.unwrap().unwrap();
    assert_eq!(model, fetched_model);
  }

  #[tokio::test]
  async fn test_fetch_model_by_unique_index<I: DbInstantiator>() {
    let db = I::init();

    let model = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test"),
      owner: Ulid::new(),
    };

    db.create_model(model.clone()).await.unwrap();

    let fetched_model = db
      .fetch_model_by_unique_index(
        "name".to_string(),
        EitherSlug::Strict(model.name.clone()),
      )
      .await
      .unwrap()
      .unwrap();
    assert_eq!(model, fetched_model);
  }

  #[tokio::test]
  async fn test_enumerate_models<I: DbInstantiator>() {
    let db = I::init();

    let model1 = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test1"),
      owner: Ulid::new(),
    };
    let model2 = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test2"),
      owner: Ulid::new(),
    };

    db.create_model(model1.clone()).await.unwrap();
    db.create_model(model2.clone()).await.unwrap();

    let models = db.enumerate_models().await.unwrap();
    assert_eq!(models.len(), 2);
    assert!(models.contains(&model1));
    assert!(models.contains(&model2));
  }

  #[tokio::test]
  async fn test_fetch_model_by_id_not_found<I: DbInstantiator>() {
    let db = I::init();

    let model = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test"),
      owner: Ulid::new(),
    };

    let fetched_model = db.fetch_model_by_id(model.id()).await.unwrap();
    assert!(fetched_model.is_none());
  }

  #[tokio::test]
  async fn test_fetch_model_by_unique_index_not_found<I: DbInstantiator>() {
    let db = I::init();

    let model = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test"),
      owner: Ulid::new(),
    };

    db.create_model(model.clone()).await.unwrap();

    let fetched_model: Option<TestModel> = db
      .fetch_model_by_unique_index(
        "name".to_string(),
        EitherSlug::Strict(StrictSlug::new("not_test")),
      )
      .await
      .unwrap();
    assert!(fetched_model.is_none());
  }

  #[tokio::test]
  async fn test_fetch_model_by_unique_index_does_not_exist<
    I: DbInstantiator,
  >() {
    let db = I::init();

    let model = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test"),
      owner: Ulid::new(),
    };

    db.create_model(model.clone()).await.unwrap();

    let result: Result<Option<TestModel>, _> = db
      .fetch_model_by_unique_index(
        "not_name".to_string(),
        EitherSlug::Strict(StrictSlug::new("test")),
      )
      .await;
    assert!(matches!(
      result,
      Err(FetchModelByIndexError::IndexDoesNotExistOnModel { .. })
    ));
  }

  #[tokio::test]
  async fn test_fetch_model_by_index<I: DbInstantiator>() {
    let db = I::init();

    let owner = Ulid::new();
    let second_owner = Ulid::new();
    let model = TestModel {
      id: model::RecordId::new(),
      name: StrictSlug::new("test"),
      owner,
    };
    let model2 = TestModel {
      id: model::RecordId::new(),
      name: StrictSlug::new("test2"),
      owner,
    };
    let model3 = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test3"),
      owner: second_owner,
    };

    db.create_model(model.clone()).await.unwrap();
    db.create_model(model2.clone()).await.unwrap();
    db.create_model(model3.clone()).await.unwrap();

    let fetched_models = db
      .fetch_model_by_index(
        "owner".to_string(),
        EitherSlug::Strict(StrictSlug::new(owner)),
      )
      .await
      .unwrap();

    assert!(fetched_models.contains(&model));
    assert!(fetched_models.contains(&model2));
    assert!(!fetched_models.contains(&model3));

    let fetched_models = db
      .fetch_model_by_index(
        "owner".to_string(),
        EitherSlug::Strict(StrictSlug::new(second_owner)),
      )
      .await
      .unwrap();

    assert!(!fetched_models.contains(&model));
    assert!(!fetched_models.contains(&model2));
    assert!(fetched_models.contains(&model3));
  }

  #[tokio::test]
  async fn test_create_model_already_exists<I: DbInstantiator>() {
    let db = I::init();

    let model = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test"),
      owner: Ulid::new(),
    };

    db.create_model(model.clone()).await.unwrap();

    let result = db.create_model(model.clone()).await;
    assert!(matches!(result, Err(CreateModelError::ModelAlreadyExists)));
  }

  #[tokio::test]
  async fn test_create_model_index_already_exists<I: DbInstantiator>() {
    let db = I::init();

    let model = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test"),
      owner: Ulid::new(),
    };
    let model2 = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test"),
      owner: Ulid::new(),
    };

    db.create_model(model.clone()).await.unwrap();

    let result = db.create_model(model2).await;

    assert!(matches!(
      result,
      Err(CreateModelError::UniqueIndexAlreadyExists { .. })
    ));
  }
  #[tokio::test]
  async fn test_patch_model<I: DbInstantiator>() {
    let db = I::init();

    let model = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test"),
      owner: Ulid::new(),
    };

    db.create_model(model.clone()).await.unwrap();

    let updated_model = TestModel {
      id:    model.id(),
      name:  StrictSlug::new("updated_test"),
      owner: Ulid::new(),
    };

    let patched_model = db
      .patch_model(model.id(), updated_model.clone())
      .await
      .unwrap();
    assert_eq!(updated_model, patched_model);

    let fetched_model =
      db.fetch_model_by_id(model.id()).await.unwrap().unwrap();
    assert_eq!(updated_model, fetched_model);
  }

  #[tokio::test]
  async fn test_patch_model_not_found<I: DbInstantiator>() {
    let db = I::init();

    let model = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test"),
      owner: Ulid::new(),
    };

    let result = db.patch_model(model.id(), model).await;
    assert!(matches!(result, Err(crate::PatchModelError::ModelNotFound)));
  }

  #[tokio::test]
  async fn test_patch_model_unique_index_conflict<I: DbInstantiator>() {
    let db = I::init();

    let model1 = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test1"),
      owner: Ulid::new(),
    };
    let model2 = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test2"),
      owner: Ulid::new(),
    };

    db.create_model(model1.clone()).await.unwrap();
    db.create_model(model2.clone()).await.unwrap();

    // Try to update model2 to have the same name as model1
    let conflicting_model = TestModel {
      id:    model2.id(),
      name:  StrictSlug::new("test1"), // This conflicts with model1
      owner: Ulid::new(),
    };

    let result = db.patch_model(model2.id(), conflicting_model).await;
    assert!(matches!(
      result,
      Err(crate::PatchModelError::UniqueIndexAlreadyExists { .. })
    ));
  }

  #[tokio::test]
  async fn test_patch_model_same_unique_index_value<I: DbInstantiator>() {
    let db = I::init();

    let model = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test"),
      owner: Ulid::new(),
    };

    db.create_model(model.clone()).await.unwrap();

    // Update the model but keep the same unique index value (name)
    let updated_model = TestModel {
      id:    model.id(),
      name:  StrictSlug::new("test"), // Same name
      owner: Ulid::new(),             // Different owner
    };

    let result = db.patch_model(model.id(), updated_model.clone()).await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), updated_model);
  }

  #[tokio::test]
  async fn test_patch_model_updates_indices<I: DbInstantiator>() {
    let db = I::init();

    let owner1 = Ulid::new();
    let owner2 = Ulid::new();

    let model = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test"),
      owner: owner1,
    };

    db.create_model(model.clone()).await.unwrap();

    // Verify the model can be found by the original owner index
    let found_models = db
      .fetch_model_by_index(
        "owner".to_string(),
        EitherSlug::Strict(StrictSlug::new(owner1.to_string())),
      )
      .await
      .unwrap();
    assert_eq!(found_models.len(), 1);
    assert!(found_models.contains(&model));

    // Update the model with a different owner
    let updated_model = TestModel {
      id:    model.id(),
      name:  StrictSlug::new("updated_test"),
      owner: owner2,
    };

    db.patch_model(model.id(), updated_model.clone())
      .await
      .unwrap();

    // Verify the old index no longer contains the model
    let old_index_models = db
      .fetch_model_by_index(
        "owner".to_string(),
        EitherSlug::Strict(StrictSlug::new(owner1.to_string())),
      )
      .await
      .unwrap();
    assert_eq!(old_index_models.len(), 0);

    // Verify the new index contains the updated model
    let new_index_models = db
      .fetch_model_by_index(
        "owner".to_string(),
        EitherSlug::Strict(StrictSlug::new(owner2.to_string())),
      )
      .await
      .unwrap();
    assert_eq!(new_index_models.len(), 1);
    assert!(new_index_models.contains(&updated_model));

    // Verify unique index is also updated
    let found_by_old_name = db
      .fetch_model_by_unique_index(
        "name".to_string(),
        EitherSlug::Strict(model.name.clone()),
      )
      .await
      .unwrap();
    assert!(found_by_old_name.is_none());

    let found_by_new_name = db
      .fetch_model_by_unique_index(
        "name".to_string(),
        EitherSlug::Strict(updated_model.name.clone()),
      )
      .await
      .unwrap();
    assert_eq!(found_by_new_name.unwrap(), updated_model);
  }

  #[tokio::test]
  async fn test_delete_model<I: DbInstantiator>() {
    let db = I::init();

    let model = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test"),
      owner: Ulid::new(),
    };

    db.create_model(model.clone()).await.unwrap();

    let deleted = db.delete_model(model.id()).await.unwrap();
    assert!(deleted);

    // Verify the model is no longer fetchable
    let fetched_model = db.fetch_model_by_id(model.id()).await.unwrap();
    assert!(fetched_model.is_none());
  }

  #[tokio::test]
  async fn test_delete_model_not_found<I: DbInstantiator>() {
    let db = I::init();

    let model = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test"),
      owner: Ulid::new(),
    };

    let deleted = db.delete_model(model.id()).await.unwrap();
    assert!(!deleted);
  }

  #[tokio::test]
  async fn test_delete_model_cleans_up_indices<I: DbInstantiator>() {
    let db = I::init();

    let owner = Ulid::new();
    let model1 = TestModel {
      id: model::RecordId::new(),
      name: StrictSlug::new("test1"),
      owner,
    };
    let model2 = TestModel {
      id: model::RecordId::new(),
      name: StrictSlug::new("test2"),
      owner,
    };

    db.create_model(model1.clone()).await.unwrap();
    db.create_model(model2.clone()).await.unwrap();

    // Verify both models are in the index
    let models_by_owner = db
      .fetch_model_by_index(
        "owner".to_string(),
        EitherSlug::Strict(StrictSlug::new(owner.to_string())),
      )
      .await
      .unwrap();
    assert_eq!(models_by_owner.len(), 2);

    // Delete one model
    let deleted = db.delete_model(model1.id()).await.unwrap();
    assert!(deleted);

    // Verify the index is updated
    let remaining_models = db
      .fetch_model_by_index(
        "owner".to_string(),
        EitherSlug::Strict(StrictSlug::new(owner.to_string())),
      )
      .await
      .unwrap();
    assert_eq!(remaining_models.len(), 1);
    assert!(remaining_models.contains(&model2));
    assert!(!remaining_models.contains(&model1));

    // Verify unique index is also cleaned up
    let found_by_name = db
      .fetch_model_by_unique_index(
        "name".to_string(),
        EitherSlug::Strict(model1.name.clone()),
      )
      .await
      .unwrap();
    assert!(found_by_name.is_none());

    // But model2 should still be findable by its unique index
    let found_model2 = db
      .fetch_model_by_unique_index(
        "name".to_string(),
        EitherSlug::Strict(model2.name.clone()),
      )
      .await
      .unwrap();
    assert_eq!(found_model2.unwrap(), model2);
  }

  #[tokio::test]
  async fn test_delete_model_cleans_up_empty_index_entries<
    I: DbInstantiator,
  >() {
    let db = I::init();

    let owner = Ulid::new();
    let model = TestModel {
      id: model::RecordId::new(),
      name: StrictSlug::new("test"),
      owner,
    };

    db.create_model(model.clone()).await.unwrap();

    // Verify the model is in the index
    let models_by_owner = db
      .fetch_model_by_index(
        "owner".to_string(),
        EitherSlug::Strict(StrictSlug::new(owner.to_string())),
      )
      .await
      .unwrap();
    assert_eq!(models_by_owner.len(), 1);

    // Delete the model
    let deleted = db.delete_model(model.id()).await.unwrap();
    assert!(deleted);

    // Verify the index entry is empty (should return empty vec, not error)
    let remaining_models = db
      .fetch_model_by_index(
        "owner".to_string(),
        EitherSlug::Strict(StrictSlug::new(owner.to_string())),
      )
      .await
      .unwrap();
    assert_eq!(remaining_models.len(), 0);
  }

  #[tokio::test]
  async fn test_enumerate_models_after_patch_and_delete<I: DbInstantiator>() {
    let db = I::init();

    let model1 = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test1"),
      owner: Ulid::new(),
    };
    let model2 = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test2"),
      owner: Ulid::new(),
    };
    let model3 = TestModel {
      id:    model::RecordId::new(),
      name:  StrictSlug::new("test3"),
      owner: Ulid::new(),
    };

    db.create_model(model1.clone()).await.unwrap();
    db.create_model(model2.clone()).await.unwrap();
    db.create_model(model3.clone()).await.unwrap();

    let all_models = db.enumerate_models().await.unwrap();
    assert_eq!(all_models.len(), 3);

    // Patch model2
    let updated_model2 = TestModel {
      id:    model2.id(),
      name:  StrictSlug::new("updated_test2"),
      owner: Ulid::new(),
    };
    db.patch_model(model2.id(), updated_model2.clone())
      .await
      .unwrap();

    // Delete model3
    db.delete_model(model3.id()).await.unwrap();

    let final_models = db.enumerate_models().await.unwrap();
    assert_eq!(final_models.len(), 2);
    assert!(final_models.contains(&model1));
    assert!(final_models.contains(&updated_model2));
    assert!(!final_models.contains(&model2)); // Old version shouldn't be there
    assert!(!final_models.contains(&model3)); // Deleted model shouldn't be
                                              // there
  }

  #[instantiate_tests(<MockInstantiator>)]
  mod test_db_mock {}

  #[instantiate_tests(<KvMockedInstantiator>)]
  mod test_db_kv_mocked {}
}
