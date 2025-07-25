use std::{fmt, path::PathBuf, str::FromStr};

use serde::{Deserialize, Serialize};

/// A path in temp storage.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TempStoragePath {
  path: PathBuf,
  size: FileSize,
}

impl TempStoragePath {
  /// Creates a new temporary storage path.
  pub fn new_random(size: FileSize) -> Self {
    Self {
      path: PathBuf::from_str(&ulid::Ulid::new().to_string()).unwrap(),
      size,
    }
  }
  /// Returns the path.
  pub fn path(&self) -> &PathBuf { &self.path }
  /// Returns the size.
  pub fn size(&self) -> &FileSize { &self.size }
  /// Sets the size.
  pub fn set_size(&mut self, size: FileSize) { self.size = size; }
}

/// An entity's visibility.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Visibility {
  /// The entity is public.
  Public,
  /// The entity is private.
  Private,
}

impl Visibility {
  /// Returns `true` if the entity is public.
  pub fn is_public(&self) -> bool { matches!(self, Visibility::Public) }
}

impl fmt::Display for Visibility {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      Visibility::Public => write!(f, "Public"),
      Visibility::Private => write!(f, "Private"),
    }
  }
}

/// The size of a file.
#[nutype::nutype(derive(
  Copy,
  Clone,
  Serialize,
  Deserialize,
  PartialEq,
  Eq,
  Hash,
  AsRef,
))]
pub struct FileSize(u64);

impl fmt::Display for FileSize {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    const KB: u64 = 1000;
    const MB: u64 = 1000 * KB;
    const GB: u64 = 1000 * MB;

    match *self.as_ref() {
      size if size < KB => write!(f, "{size} B"),
      size if size < MB => write!(f, "{:.2} KB", size as f64 / KB as f64),
      size if size < GB => {
        write!(f, "{:.2} MB", size as f64 / MB as f64)
      }
      size => write!(f, "{:.2} GB", size as f64 / GB as f64),
    }
  }
}

impl fmt::Debug for FileSize {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_tuple("FileSize").field(&self.to_string()).finish()
  }
}
