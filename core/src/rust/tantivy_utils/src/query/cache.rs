//! Cached query support

use std::{hash::Hash, sync::Arc};

use quick_cache::{sync::Cache, Equivalent, Weighter};
use tantivy::{
    collector::SegmentCollector,
    query::{EnableScoring, Query, Weight},
    schema::{Field, Schema},
    Searcher, SegmentId, TantivyError,
};
use tantivy_common::BitSet;

use crate::collectors::limited_collector::{
    LimitCounter, LimitedCollector, LimitedSegmentCollector,
};

use super::bitset_weight::BitSetWeight;

/// Cache for query results
pub struct QueryCache<QueryType, WeighterType>
where
    QueryType: CachableQuery,
    WeighterType: Weighter<(SegmentId, QueryType), Arc<BitSet>> + Default + Clone,
{
    // Cache of query -> docs
    cache: Cache<(SegmentId, QueryType), Arc<BitSet>, WeighterType>,
}

/// Trait for cachable query keys
pub trait CachableQuery: Eq + PartialEq + Hash + Clone {
    /// Should this query be cached?
    fn should_cache(&self) -> bool;

    fn to_query(
        &self,
        schema: &Schema,
        default_field: Option<Field>,
    ) -> Result<Box<dyn Query>, TantivyError>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CachableQueryKey<'a, QueryType>(pub SegmentId, pub &'a QueryType)
where
    QueryType: Clone + PartialEq + Eq;

impl<'a, QueryType> From<CachableQueryKey<'a, QueryType>> for (SegmentId, QueryType)
where
    QueryType: Clone + PartialEq + Eq,
{
    fn from(value: CachableQueryKey<'a, QueryType>) -> Self {
        (value.0, value.1.clone())
    }
}

impl<'a, QueryType> Equivalent<(SegmentId, QueryType)> for CachableQueryKey<'a, QueryType>
where
    QueryType: Clone + PartialEq + Eq,
{
    fn equivalent(&self, key: &(SegmentId, QueryType)) -> bool {
        self.0 == key.0 && *self.1 == key.1
    }
}

// Tuning parameters for query cache
const QUERY_CACHE_MAX_SIZE_BYTES: u64 = 50_000_000;
// Rough estimate of bitset size - 250k docs
const QUERY_CACHE_AVG_ITEM_SIZE: u64 = 31250;
const QUERY_CACHE_ESTIMATED_ITEM_COUNT: u64 =
    QUERY_CACHE_MAX_SIZE_BYTES / QUERY_CACHE_AVG_ITEM_SIZE;

impl<QueryType, WeighterType> QueryCache<QueryType, WeighterType>
where
    QueryType: CachableQuery,
    WeighterType: Weighter<(SegmentId, QueryType), Arc<BitSet>> + Default + Clone,
{
    pub fn new(estimated_items_count: u64, weight_capacity: u64) -> Self {
        Self {
            cache: Cache::with_weighter(
                estimated_items_count as usize,
                weight_capacity,
                WeighterType::default(),
            ),
        }
    }

    pub fn query_cache_stats(&self) -> (u64, u64) {
        (self.cache.hits(), self.cache.misses())
    }

    /// Gets the current cache size, in bytes
    pub fn size(&self) -> u64 {
        self.cache.weight()
    }

    /// Execute a cachable query
    pub fn search<C>(
        &self,
        searcher: &Searcher,
        schema: &Schema,
        default_field: Option<Field>,
        cachable_query: QueryType,
        collector: C,
    ) -> Result<C::Fruit, TantivyError>
    where
        C: LimitedCollector,
        C::Child: LimitedSegmentCollector,
    {
        let scoring = EnableScoring::disabled_from_searcher(searcher);

        let mut query_weight: Option<Box<dyn Weight>> = None;

        let segment_readers = searcher.segment_readers();
        let mut fruits: Vec<<C::Child as SegmentCollector>::Fruit> =
            Vec::with_capacity(segment_readers.len());

        let mut limiter = LimitCounter::new(collector.limit());

        // Note - the query optimizations here only work for the single threaded querying.  That matches
        // the pattern FiloDB uses because it will dispatch multiple queries at a time on different threads,
        // so this results in net improvement anyway.  If we need to change to the multithreaded executor
        // in the future then the lazy query evaluation code will need some work
        for (segment_ord, segment_reader) in segment_readers.iter().enumerate() {
            // Is it cached
            let cache_key = CachableQueryKey(segment_reader.segment_id(), &cachable_query);

            let docs = if let Some(docs) = self.cache.get(&cache_key) {
                // Cache hit
                docs
            } else {
                // Build query if needed.  We do this lazily as it may be expensive to parse a regex, for example.
                // This can give a 2-4x speedup in some cases.
                let weight = if let Some(weight) = &query_weight {
                    weight
                } else {
                    let query = cachable_query.to_query(schema, default_field)?;
                    let weight = query.weight(scoring)?;

                    query_weight = Some(weight);

                    // Unwrap is safe here because we just set the value
                    #[allow(clippy::unwrap_used)]
                    query_weight.as_ref().unwrap()
                };

                // Load bit set
                let mut bitset = BitSet::with_max_value(segment_reader.max_doc());

                weight.for_each_no_score(segment_reader, &mut |docs| {
                    for doc in docs.iter().cloned() {
                        bitset.insert(doc);
                    }
                })?;

                let bitset = Arc::new(bitset);

                if cachable_query.should_cache() {
                    self.cache.insert(cache_key.into(), bitset.clone());
                }

                bitset
            };

            let weight = BitSetWeight::new(docs);
            let results = collector.collect_segment_with_limiter(
                &weight,
                segment_ord as u32,
                segment_reader,
                &mut limiter,
            )?;

            fruits.push(results);

            if limiter.at_limit() {
                break;
            }
        }

        collector.merge_fruits(fruits)
    }
}

impl<QueryType, WeighterType> Default for QueryCache<QueryType, WeighterType>
where
    QueryType: CachableQuery,
    WeighterType: Weighter<(SegmentId, QueryType), Arc<BitSet>> + Default + Clone,
{
    fn default() -> Self {
        Self::new(QUERY_CACHE_ESTIMATED_ITEM_COUNT, QUERY_CACHE_MAX_SIZE_BYTES)
    }
}

#[cfg(test)]
mod tests {
    use std::hash::{DefaultHasher, Hasher};

    use tantivy::query::AllQuery;

    use crate::test_utils::build_test_schema;

    use super::*;

    #[derive(Clone, Eq, PartialEq, Hash, Debug)]
    pub enum TestQuery {
        Test(u32),
    }

    impl CachableQuery for TestQuery {
        fn should_cache(&self) -> bool {
            true
        }

        fn to_query(
            &self,
            _schema: &Schema,
            _default_field: Option<Field>,
        ) -> Result<Box<dyn Query>, TantivyError> {
            Ok(Box::new(AllQuery))
        }
    }

    #[test]
    fn test_cache_key_equivilance() {
        let index = build_test_schema();
        let reader = index.searcher.segment_readers().first().unwrap();

        let query = TestQuery::Test(1234);

        let key = CachableQueryKey(reader.segment_id(), &query);
        let owned_key: (SegmentId, TestQuery) = key.clone().into();

        assert_eq!(key.0, owned_key.0);
        assert_eq!(*key.1, owned_key.1);

        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        let key_hash = hasher.finish();

        let mut hasher = DefaultHasher::new();
        owned_key.hash(&mut hasher);
        let owned_key_hash = hasher.finish();

        assert_eq!(key_hash, owned_key_hash);
    }
}
