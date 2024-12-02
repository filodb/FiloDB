//! Cache for fast field columns

use std::sync::Arc;

use quick_cache::{sync::Cache, Equivalent};
use tantivy::{
    columnar::{BytesColumn, Column, DynamicColumn, HasAssociatedColumnType, StrColumn},
    SegmentId, SegmentReader,
};

// Max column items to cache.  These are relatively cheap (< 1KB)
// 1 item per column, per segment
const DEFAULT_COLUMN_CACHE_ITEM_COUNT: usize = 1000;

// Helper to avoid having to clone strings just to do a cache lookup
#[derive(Hash, PartialEq, Eq, Debug, Clone)]
struct CacheKey<'a>(SegmentId, &'a str);

impl<'a> From<CacheKey<'a>> for (SegmentId, String) {
    fn from(value: CacheKey<'a>) -> Self {
        (value.0, value.1.to_string())
    }
}

impl Equivalent<(SegmentId, String)> for CacheKey<'_> {
    fn equivalent(&self, key: &(SegmentId, String)) -> bool {
        self.0 == key.0 && self.1 == key.1
    }
}

/// Opening DynamicColumn instances requires parsing some headers
/// and other items that while fast, can add up if you're doing this
/// thousands of times a second.  Since columns for a given segment
/// are immutable once created caching this parsed data is safe
/// and cheap and can result in major speedups on things like
/// point queries.
#[derive(Clone)]
pub struct ColumnCache {
    cache: Arc<Cache<(SegmentId, String), DynamicColumn>>,
}

impl Default for ColumnCache {
    fn default() -> Self {
        Self::new(DEFAULT_COLUMN_CACHE_ITEM_COUNT)
    }
}

impl ColumnCache {
    pub fn new(size: usize) -> Self {
        Self {
            cache: Arc::new(Cache::new(size)),
        }
    }

    pub fn clear(&self) {
        self.cache.clear();
    }

    pub fn stats(&self) -> (u64, u64) {
        (self.cache.hits(), self.cache.misses())
    }

    pub fn get_column<T>(
        &self,
        reader: &SegmentReader,
        field: &str,
    ) -> tantivy::Result<Option<Column<T>>>
    where
        T: HasAssociatedColumnType,
        DynamicColumn: From<Column<T>>,
        DynamicColumn: Into<Option<Column<T>>>,
    {
        let key = CacheKey(reader.segment_id(), field);

        if let Some(col) = self.cache.get(&key) {
            Ok(col.into())
        } else {
            let column: Option<Column<T>> = reader.fast_fields().column_opt(field)?;

            if let Some(col) = column {
                self.cache.insert(key.into(), col.clone().into());

                Ok(Some(col))
            } else {
                Ok(None)
            }
        }
    }

    pub fn get_bytes_column(
        &self,
        reader: &SegmentReader,
        field: &str,
    ) -> tantivy::Result<Option<BytesColumn>> {
        let key = CacheKey(reader.segment_id(), field);

        if let Some(col) = self.cache.get(&key) {
            Ok(col.into())
        } else {
            let column: Option<BytesColumn> = reader.fast_fields().bytes(field)?;

            if let Some(col) = column {
                self.cache.insert(key.into(), col.clone().into());

                Ok(Some(col))
            } else {
                Ok(None)
            }
        }
    }

    pub fn get_str_column(
        &self,
        reader: &SegmentReader,
        field: &str,
    ) -> tantivy::Result<Option<StrColumn>> {
        let key = CacheKey(reader.segment_id(), field);

        if let Some(col) = self.cache.get(&key) {
            Ok(col.into())
        } else {
            let column: Option<StrColumn> = reader.fast_fields().str(field)?;

            if let Some(col) = column {
                self.cache.insert(key.into(), col.clone().into());

                Ok(Some(col))
            } else {
                Ok(None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::field_constants::{PART_ID, PART_KEY};
    use std::hash::{DefaultHasher, Hash, Hasher};

    use crate::test_utils::{build_test_schema, COL1_NAME};

    use super::*;

    #[test]
    fn test_cache_key_equivilance() {
        let index = build_test_schema();
        let reader = index.searcher.segment_readers().first().unwrap();

        let key = CacheKey(reader.segment_id(), "foo");
        let owned_key: (SegmentId, String) = key.clone().into();

        assert_eq!(key.0, owned_key.0);
        assert_eq!(key.1, owned_key.1);

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let key_hash = hasher.finish();

        let mut hasher = DefaultHasher::new();
        owned_key.hash(&mut hasher);
        let owned_key_hash = hasher.finish();

        assert_eq!(key_hash, owned_key_hash);
    }

    #[test]
    fn test_cache_miss() {
        let index = build_test_schema();
        let cache = ColumnCache::default();
        let reader = index.searcher.segment_readers().first().unwrap();

        let _: Column<i64> = cache
            .get_column(reader, PART_ID)
            .expect("Should succeed")
            .expect("Should return one item");

        assert_eq!(cache.cache.misses(), 1);
        assert_eq!(cache.cache.hits(), 0);
    }

    #[test]
    fn test_cache_hit() {
        let index = build_test_schema();
        let cache = ColumnCache::default();
        let reader = index.searcher.segment_readers().first().unwrap();

        let _: Column<i64> = cache
            .get_column(reader, PART_ID)
            .expect("Should succeed")
            .expect("Should return one item");

        let _: Column<i64> = cache
            .get_column(reader, PART_ID)
            .expect("Should succeed")
            .expect("Should return one item");

        assert_eq!(cache.cache.misses(), 1);
        assert_eq!(cache.cache.hits(), 1);
    }

    #[test]
    fn test_str_cache_miss() {
        let index = build_test_schema();
        let cache = ColumnCache::default();
        let reader = index.searcher.segment_readers().first().unwrap();

        let _ = cache
            .get_str_column(reader, COL1_NAME)
            .expect("Should succeed")
            .expect("Should return one item");

        assert_eq!(cache.cache.misses(), 1);
        assert_eq!(cache.cache.hits(), 0);
    }

    #[test]
    fn test_str_cache_hit() {
        let index = build_test_schema();
        let cache = ColumnCache::default();
        let reader = index.searcher.segment_readers().first().unwrap();

        let _ = cache
            .get_str_column(reader, COL1_NAME)
            .expect("Should succeed")
            .expect("Should return one item");

        let _ = cache
            .get_str_column(reader, COL1_NAME)
            .expect("Should succeed")
            .expect("Should return one item");

        assert_eq!(cache.cache.misses(), 1);
        assert_eq!(cache.cache.hits(), 1);
    }

    #[test]
    fn test_bytes_cache_miss() {
        let index = build_test_schema();
        let cache = ColumnCache::default();
        let reader = index.searcher.segment_readers().first().unwrap();

        let _ = cache
            .get_bytes_column(reader, PART_KEY)
            .expect("Should succeed")
            .expect("Should return one item");

        assert_eq!(cache.cache.misses(), 1);
        assert_eq!(cache.cache.hits(), 0);
    }

    #[test]
    fn test_bytes_cache_hit() {
        let index = build_test_schema();
        let cache = ColumnCache::default();
        let reader = index.searcher.segment_readers().first().unwrap();

        let _ = cache
            .get_bytes_column(reader, PART_KEY)
            .expect("Should succeed")
            .expect("Should return one item");

        let _ = cache
            .get_bytes_column(reader, PART_KEY)
            .expect("Should succeed")
            .expect("Should return one item");

        assert_eq!(cache.cache.misses(), 1);
        assert_eq!(cache.cache.hits(), 1);
    }
}
