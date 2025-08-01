use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Credentials for a storage backend.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum StorageCredentials {
  /// Storage credentials for local filesystem storage.
  Local(LocalStorageCredentials),
  /// Storage credentials for R2 object storage.
  R2(R2StorageCredentials),
  /// Storage credentials for in-memory storage.
  Memory(MemoryStorageCredentials),
}

/// Storage credentials for local filesystem storage.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LocalStorageCredentials(pub PathBuf);

/// Storage credentials for in-memory storage.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct MemoryStorageCredentials;

/// Storage credentials for R2 object storage.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub enum R2StorageCredentials {
  /// The default credential set for R2.
  Default {
    /// The access key ID. Corresponds directly to S3 equivalent.
    access_key:        String,
    /// The access key secret. Corresponds directly to S3 equivalent.
    secret_access_key: String,
    /// The http endpoint: `https://[account_id].r2.cloudflarestorage.com`
    endpoint:          String,
    /// The bucket name. Corresponds directly to S3 equivalent.
    bucket:            String,
  },
}
