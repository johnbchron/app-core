use std::marker::PhantomData;

use kv::EitherSlug;
use miette::{IntoDiagnostic, Report, Result};
use model::{Model, RecordId};
use sqlx::{postgres::PgPool, Postgres, Transaction};

use crate::adapter::*;

/// Serialize a model to bytes using bincode
fn serialize_model<M: Model>(model: &M) -> Result<Vec<u8>, Report> {
  rmp_serde::to_vec_named(model).into_diagnostic()
}

/// Deserialize a model from bytes using bincode
fn deserialize_model<M: Model>(data: &[u8]) -> Result<M, Report> {
  rmp_serde::from_slice(data).into_diagnostic()
}

/// PostgreSQL implementation of the DatabaseAdapter trait.
pub struct PostgresAdapter<M: Model> {
  pool:     PgPool,
  _phantom: PhantomData<M>,
}

impl<M: Model> PostgresAdapter<M> {
  /// Create a new PostgreSQL adapter with the given connection pool.
  pub fn new(pool: PgPool) -> Self {
    Self {
      pool,
      _phantom: PhantomData,
    }
  }

  /// Initialize the database schema for this model type.
  /// This should be called once during application startup.
  pub async fn initialize_schema(&self) -> Result<()> {
    let mut tx = self.pool.begin().await.into_diagnostic()?;

    // Create the main table for storing models
    let create_table_query = format!(
      r#"
            CREATE TABLE IF NOT EXISTS {} (
                id BYTEA PRIMARY KEY,
                data BYTEA NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
            "#,
      M::TABLE_NAME
    );
    sqlx::query(&create_table_query)
      .execute(&mut *tx)
      .await
      .into_diagnostic()?;

    // Create unique index table
    let unique_index_table = format!("{}_unique_indices", M::TABLE_NAME);
    let create_unique_index_query = format!(
      r#"
            CREATE TABLE IF NOT EXISTS {} (
                model_id BYTEA NOT NULL REFERENCES {} (id) ON DELETE CASCADE,
                index_name TEXT NOT NULL,
                index_value BYTEA NOT NULL,
                PRIMARY KEY (index_name, index_value)
            )
            "#,
      unique_index_table,
      M::TABLE_NAME
    );
    sqlx::query(&create_unique_index_query)
      .execute(&mut *tx)
      .await
      .into_diagnostic()?;

    // Create regular index table
    let index_table = format!("{}_indices", M::TABLE_NAME);
    let create_index_query = format!(
      r#"
            CREATE TABLE IF NOT EXISTS {} (
                model_id BYTEA NOT NULL REFERENCES {} (id) ON DELETE CASCADE,
                index_name TEXT NOT NULL,
                index_value BYTEA NOT NULL
            )
            "#,
      index_table,
      M::TABLE_NAME
    );
    sqlx::query(&create_index_query)
      .execute(&mut *tx)
      .await
      .into_diagnostic()?;

    // Create index on the index table for faster lookups
    let create_index_idx_query = format!(
      "CREATE INDEX IF NOT EXISTS {index_table}_idx ON {index_table} \
       (index_name, index_value)"
    );
    sqlx::query(&create_index_idx_query)
      .execute(&mut *tx)
      .await
      .into_diagnostic()?;

    tx.commit().await.into_diagnostic()?;
    Ok(())
  }

  /// Update indices for a model
  async fn update_indices(
    &self,
    tx: &mut Transaction<'_, Postgres>,
    model_id: &[u8],
    model: &M,
  ) -> Result<(), CreateModelError> {
    let unique_index_table = format!("{}_unique_indices", M::TABLE_NAME);
    let index_table = format!("{}_indices", M::TABLE_NAME);

    // Handle unique indices
    for (selector, getter) in M::UNIQUE_INDICES {
      let index_name = selector.to_string();
      let index_values = getter(model);

      for index_value in index_values {
        let serialized_value = index_value.to_string();

        // Check if this unique index already exists for a different model
        let existing_query = format!(
          "SELECT model_id FROM {unique_index_table} WHERE index_name = $1 \
           AND index_value = $2"
        );
        let existing: Option<(Vec<u8>,)> = sqlx::query_as(&existing_query)
          .bind(&index_name)
          .bind(serialized_value.as_bytes())
          .fetch_optional(&mut **tx)
          .await
          .into_diagnostic()
          .map_err(CreateModelError::Db)?;

        if let Some((existing_model_id,)) = existing {
          if existing_model_id != model_id {
            return Err(CreateModelError::UniqueIndexAlreadyExists {
              index_selector: index_name,
              index_value,
            });
          }
        }

        // Insert or update the unique index
        let upsert_query = format!(
          "INSERT INTO {unique_index_table} (model_id, index_name, \
           index_value) VALUES ($1, $2, $3) 
                     ON CONFLICT (index_name, index_value) DO UPDATE SET \
           model_id = $1"
        );
        sqlx::query(&upsert_query)
          .bind(model_id)
          .bind(&index_name)
          .bind(&serialized_value)
          .execute(&mut **tx)
          .await
          .into_diagnostic()
          .map_err(CreateModelError::Db)?;
      }
    }

    // Handle regular indices
    for (selector, getter) in M::INDICES {
      let index_name = selector.to_string();
      let index_values = getter(model);

      // First, remove old indices for this model and index name
      let delete_query = format!(
        "DELETE FROM {index_table} WHERE model_id = $1 AND index_name = $2"
      );
      sqlx::query(&delete_query)
        .bind(model_id)
        .bind(&index_name)
        .execute(&mut **tx)
        .await
        .into_diagnostic()
        .map_err(CreateModelError::Db)?;

      // Insert new indices
      for index_value in index_values {
        let serialized_value = index_value.to_string();

        let insert_query = format!(
          "INSERT INTO {index_table} (model_id, index_name, index_value) \
           VALUES ($1, $2, $3)"
        );
        sqlx::query(&insert_query)
          .bind(model_id)
          .bind(&index_name)
          .bind(&serialized_value)
          .execute(&mut **tx)
          .await
          .into_diagnostic()
          .map_err(CreateModelError::Db)?;
      }
    }

    Ok(())
  }

  /// Remove all indices for a model
  async fn remove_indices(
    &self,
    tx: &mut Transaction<'_, Postgres>,
    model_id: &[u8],
  ) -> Result<(), DeleteModelError> {
    let unique_index_table = format!("{}_unique_indices", M::TABLE_NAME);
    let index_table = format!("{}_indices", M::TABLE_NAME);

    // Remove unique indices
    let delete_unique_query =
      format!("DELETE FROM {unique_index_table} WHERE model_id = $1");
    sqlx::query(&delete_unique_query)
      .bind(model_id)
      .execute(&mut **tx)
      .await
      .into_diagnostic()
      .map_err(DeleteModelError::FailedToCleanupIndices)?;

    // Remove regular indices
    let delete_regular_query =
      format!("DELETE FROM {index_table} WHERE model_id = $1");
    sqlx::query(&delete_regular_query)
      .bind(model_id)
      .execute(&mut **tx)
      .await
      .into_diagnostic()
      .map_err(DeleteModelError::FailedToCleanupIndices)?;

    Ok(())
  }
}

#[async_trait::async_trait]
impl<M: Model> super::DatabaseAdapter<M> for PostgresAdapter<M> {
  async fn create_model(&self, model: M) -> Result<M, CreateModelError> {
    let model_id = model.id();
    let model_id_bytes = model_id.inner().to_bytes();
    let serialized_model =
      serialize_model(&model).map_err(CreateModelError::Serde)?;

    let mut tx = self
      .pool
      .begin()
      .await
      .into_diagnostic()
      .map_err(CreateModelError::Db)?;

    // Check if model with this ID already exists
    let exists_query = format!("SELECT 1 FROM {} WHERE id = $1", M::TABLE_NAME);
    let exists: Option<(i32,)> = sqlx::query_as(&exists_query)
      .bind(model_id_bytes)
      .fetch_optional(&mut *tx)
      .await
      .into_diagnostic()
      .map_err(CreateModelError::Db)?;

    if exists.is_some() {
      return Err(CreateModelError::ModelAlreadyExists);
    }

    // Update indices first to check for unique constraint violations
    self
      .update_indices(&mut tx, &model_id_bytes, &model)
      .await?;

    // Insert the model
    let insert_query = format!(
      "INSERT INTO {} (id, data, updated_at) VALUES ($1, $2, NOW())",
      M::TABLE_NAME
    );
    sqlx::query(&insert_query)
      .bind(model_id_bytes)
      .bind(&serialized_model)
      .execute(&mut *tx)
      .await
      .into_diagnostic()
      .map_err(CreateModelError::Db)?;

    tx.commit()
      .await
      .into_diagnostic()
      .map_err(CreateModelError::Db)?;

    Ok(model)
  }

  async fn fetch_model_by_id(
    &self,
    id: RecordId<M>,
  ) -> Result<Option<M>, FetchModelError> {
    let query = format!("SELECT data FROM {} WHERE id = $1", M::TABLE_NAME);
    let row: Option<(Vec<u8>,)> = sqlx::query_as(&query)
      .bind(id.inner().to_bytes())
      .fetch_optional(&self.pool)
      .await
      .into_diagnostic()
      .map_err(FetchModelError::Db)?;

    match row {
      Some((data,)) => {
        let model = deserialize_model(&data).map_err(FetchModelError::Serde)?;

        Ok(Some(model))
      }
      None => Ok(None),
    }
  }

  async fn fetch_model_by_unique_index(
    &self,
    index_selector: M::UniqueIndexSelector,
    index_value: EitherSlug,
  ) -> Result<Option<M>, FetchModelByIndexError> {
    let unique_index_table = format!("{}_unique_indices", M::TABLE_NAME);
    let index_name = index_selector.to_string();
    let serialized_value = index_value.to_string();

    // First, find the model ID from the unique index
    let find_id_query = format!(
      "SELECT model_id FROM {unique_index_table} WHERE index_name = $1 AND \
       index_value = $2"
    );
    let model_id_row: Option<(Vec<u8>,)> = sqlx::query_as(&find_id_query)
      .bind(&index_name)
      .bind(&serialized_value)
      .fetch_optional(&self.pool)
      .await
      .into_diagnostic()
      .map_err(FetchModelByIndexError::Db)?;

    let model_id_bytes = match model_id_row {
      Some((id,)) => id,
      None => return Ok(None),
    };

    // Now fetch the actual model data
    let fetch_query =
      format!("SELECT data FROM {} WHERE id = $1", M::TABLE_NAME);
    let model_row: Option<(Vec<u8>,)> = sqlx::query_as(&fetch_query)
      .bind(&model_id_bytes)
      .fetch_optional(&self.pool)
      .await
      .into_diagnostic()
      .map_err(FetchModelByIndexError::Db)?;

    match model_row {
      Some((data,)) => {
        let model =
          deserialize_model(&data).map_err(FetchModelByIndexError::Serde)?;
        Ok(Some(model))
      }
      None => Ok(None),
    }
  }

  async fn fetch_ids_by_index(
    &self,
    index_selector: M::IndexSelector,
    index_value: EitherSlug,
  ) -> Result<Vec<RecordId<M>>, FetchModelByIndexError> {
    let index_table = format!("{}_indices", M::TABLE_NAME);
    let index_name = index_selector.to_string();
    let serialized_value = index_value.to_string();

    let query = format!(
      "SELECT model_id FROM {index_table} WHERE index_name = $1 AND \
       index_value = $2"
    );
    let rows: Vec<(Vec<u8>,)> = sqlx::query_as(&query)
      .bind(&index_name)
      .bind(&serialized_value)
      .fetch_all(&self.pool)
      .await
      .into_diagnostic()
      .map_err(FetchModelByIndexError::Db)?;

    let ids = rows
      .into_iter()
      .filter_map(|(id_bytes,)| RecordId::try_from(id_bytes.as_ref()).ok())
      .collect();

    Ok(ids)
  }

  async fn count_models_by_index(
    &self,
    index_selector: M::IndexSelector,
    index_value: EitherSlug,
  ) -> Result<u32, FetchModelByIndexError> {
    let index_table = format!("{}_indices", M::TABLE_NAME);
    let index_name = index_selector.to_string();
    let serialized_value = index_value.to_string();

    let query = format!(
      "SELECT COUNT(*) FROM {index_table} WHERE index_name = $1 AND \
       index_value = $2"
    );
    let count: (i64,) = sqlx::query_as(&query)
      .bind(&index_name)
      .bind(&serialized_value)
      .fetch_one(&self.pool)
      .await
      .into_diagnostic()
      .map_err(FetchModelByIndexError::Db)?;

    Ok(count.0 as u32)
  }

  async fn enumerate_models(&self) -> Result<Vec<M>> {
    let query =
      format!("SELECT data FROM {} ORDER BY created_at", M::TABLE_NAME);
    let rows: Vec<(Vec<u8>,)> = sqlx::query_as(&query)
      .fetch_all(&self.pool)
      .await
      .into_diagnostic()?;

    let mut models = Vec::new();
    for (data,) in rows {
      let model =
        deserialize_model(&data).map_err(FetchModelByIndexError::Serde)?;
      models.push(model);
    }

    Ok(models)
  }

  async fn patch_model(
    &self,
    id: RecordId<M>,
    model: M,
  ) -> Result<M, PatchModelError> {
    let model_id_bytes = id.inner().to_bytes();
    let serialized_model =
      serialize_model(&model).map_err(PatchModelError::Serde)?;

    let mut tx = self
      .pool
      .begin()
      .await
      .into_diagnostic()
      .map_err(PatchModelError::Db)?;

    // Check if model exists
    let exists_query = format!("SELECT 1 FROM {} WHERE id = $1", M::TABLE_NAME);
    let exists: Option<(i32,)> = sqlx::query_as(&exists_query)
      .bind(model_id_bytes)
      .fetch_optional(&mut *tx)
      .await
      .into_diagnostic()
      .map_err(PatchModelError::Db)?;

    if exists.is_none() {
      return Err(PatchModelError::ModelNotFound);
    }

    // Remove old indices
    self
      .remove_indices(&mut tx, &model_id_bytes)
      .await
      .map_err(|e| match e {
        DeleteModelError::FailedToCleanupIndices(report) => {
          PatchModelError::Db(report)
        }
        DeleteModelError::Serde(report) => PatchModelError::Serde(report),
        DeleteModelError::RetryableTransaction(report) => {
          PatchModelError::RetryableTransaction(report)
        }
        DeleteModelError::Db(report) => PatchModelError::Db(report),
      })?;

    // Update indices with new values
    self
      .update_indices(&mut tx, &model_id_bytes, &model)
      .await
      .map_err(|e| match e {
        CreateModelError::UniqueIndexAlreadyExists {
          index_selector,
          index_value,
        } => PatchModelError::UniqueIndexAlreadyExists {
          index_selector,
          index_value,
        },
        CreateModelError::Serde(report) => PatchModelError::Serde(report),
        CreateModelError::RetryableTransaction(report) => {
          PatchModelError::RetryableTransaction(report)
        }
        CreateModelError::Db(report) => PatchModelError::Db(report),
        CreateModelError::ModelAlreadyExists => {
          // This shouldn't happen in patch context
          unreachable!()
        }
      })?;

    // Update the model data
    let update_query = format!(
      "UPDATE {} SET data = $1, updated_at = NOW() WHERE id = $2",
      M::TABLE_NAME
    );
    sqlx::query(&update_query)
      .bind(&serialized_model)
      .bind(model_id_bytes)
      .execute(&mut *tx)
      .await
      .into_diagnostic()
      .map_err(PatchModelError::Db)?;

    tx.commit()
      .await
      .into_diagnostic()
      .map_err(PatchModelError::Db)?;

    Ok(model)
  }

  async fn delete_model(
    &self,
    id: RecordId<M>,
  ) -> Result<bool, DeleteModelError> {
    let model_id_bytes = id.inner().to_bytes();

    let mut tx = self
      .pool
      .begin()
      .await
      .into_diagnostic()
      .map_err(DeleteModelError::Db)?;

    // Check if model exists
    let exists_query = format!("SELECT 1 FROM {} WHERE id = $1", M::TABLE_NAME);
    let exists: Option<(i32,)> = sqlx::query_as(&exists_query)
      .bind(model_id_bytes)
      .fetch_optional(&mut *tx)
      .await
      .into_diagnostic()
      .map_err(DeleteModelError::Db)?;

    if exists.is_none() {
      return Ok(false);
    }

    // Remove indices (this will cascade due to foreign key constraints, but we
    // do it explicitly)
    self.remove_indices(&mut tx, &model_id_bytes).await?;

    // Delete the model
    let delete_query = format!("DELETE FROM {} WHERE id = $1", M::TABLE_NAME);
    let result = sqlx::query(&delete_query)
      .bind(model_id_bytes)
      .execute(&mut *tx)
      .await
      .into_diagnostic()
      .map_err(DeleteModelError::Db)?;

    tx.commit()
      .await
      .into_diagnostic()
      .map_err(DeleteModelError::Db)?;

    Ok(result.rows_affected() > 0)
  }
}
