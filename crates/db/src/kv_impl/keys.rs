use std::sync::LazyLock;

use kv::*;

static UNIQUE_INDEX_NS_SEGMENT: LazyLock<StrictSlug> =
  LazyLock::new(|| StrictSlug::new("unique_index"));
static INDEX_NS_SEGMENT: LazyLock<StrictSlug> =
  LazyLock::new(|| StrictSlug::new("index"));
static MODEL_NS_SEGMENT: LazyLock<StrictSlug> =
  LazyLock::new(|| StrictSlug::new("model"));

pub(crate) fn model_base_key<M: model::Model>(id: &model::RecordId<M>) -> Key {
  let id_ulid: model::Ulid = (*id).into();
  Key::new_lazy(&MODEL_NS_SEGMENT)
    .with(StrictSlug::new(M::TABLE_NAME))
    .with(StrictSlug::new(id_ulid))
}

pub(crate) fn unique_index_base_key<M: model::Model>(
  index_selector: M::UniqueIndexSelector,
) -> Key {
  Key::new_lazy(&UNIQUE_INDEX_NS_SEGMENT)
    .with(StrictSlug::new(M::TABLE_NAME))
    .with(StrictSlug::new(index_selector.to_string()))
}

pub(crate) fn index_base_key<M: model::Model>(
  index_selector: M::IndexSelector,
) -> Key {
  Key::new_lazy(&INDEX_NS_SEGMENT)
    .with(StrictSlug::new(M::TABLE_NAME))
    .with(StrictSlug::new(index_selector.to_string()))
}
