//! Cachable query implementation

use std::{ops::Bound, sync::Arc};

use quick_cache::Weighter;
use tantivy::{
    query::{AllQuery, Query, RangeQuery, TermQuery, TermSetQuery},
    schema::{Field, IndexRecordOption, Schema},
    SegmentId, TantivyError, Term,
};
use tantivy_common::BitSet;
use tantivy_utils::field_constants;

use super::parse_query;

/// A query that can potentially be cached
///
/// We can't just hold a reference to Tantivy's Query object because
/// they don't implement Hash/Equals so they can't be a key
#[derive(Debug, Hash, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum CachableQuery {
    /// A complex query that is serialized in byte form
    Complex(Arc<Box<[u8]>>),
    /// Search by part key
    ByPartKey(Arc<Box<[u8]>>),
    /// Search by list of part IDs
    ByPartIds(Arc<Box<[i32]>>),
    /// Search by end time
    ByEndTime(i64),
    /// Search for single part ID (not cached)
    ByPartId(i32),
    /// All docs query (not cached)
    All,
}

impl tantivy_utils::query::cache::CachableQuery for CachableQuery {
    fn should_cache(&self) -> bool {
        match self {
            CachableQuery::Complex(_) => true,
            CachableQuery::ByPartIds(_) => true,
            CachableQuery::ByEndTime(_) => true,
            // No point caching all docs - the "query" is constant time anyway
            &CachableQuery::All => false,
            // A single term lookup is very efficient - no benefit in caching the doc ID
            CachableQuery::ByPartId(_) => false,
            // Also single term lookup
            CachableQuery::ByPartKey(_) => false,
        }
    }

    fn to_query(
        &self,
        schema: &Schema,
        default_field: Option<Field>,
    ) -> Result<Box<dyn Query>, TantivyError> {
        match self {
            CachableQuery::Complex(query_bytes) => {
                let (_, query) = parse_query(query_bytes, schema, default_field)
                    .map_err(|e| TantivyError::InternalError(format!("{:#}", e)))?;

                Ok(query)
            }
            CachableQuery::ByPartKey(part_key) => {
                let field = schema.get_field(field_constants::PART_KEY)?;
                let term = Term::from_field_bytes(field, part_key);
                let query = TermQuery::new(term, IndexRecordOption::Basic);

                Ok(Box::new(query))
            }
            CachableQuery::ByPartIds(part_ids) => {
                let part_id_field = schema.get_field(field_constants::PART_ID)?;

                let mut terms = vec![];
                for id in part_ids.iter() {
                    let term = Term::from_field_i64(part_id_field, *id as i64);
                    terms.push(term);
                }

                let query = TermSetQuery::new(terms);

                Ok(Box::new(query))
            }
            CachableQuery::All => Ok(Box::new(AllQuery)),
            CachableQuery::ByPartId(part_id) => {
                let part_id_field = schema.get_field(field_constants::PART_ID)?;
                let term = Term::from_field_i64(part_id_field, *part_id as i64);

                let query = TermQuery::new(term, IndexRecordOption::Basic);

                Ok(Box::new(query))
            }
            CachableQuery::ByEndTime(ended_before) => {
                let query = RangeQuery::new_i64_bounds(
                    field_constants::END_TIME.to_string(),
                    Bound::Included(0),
                    Bound::Included(*ended_before),
                );

                Ok(Box::new(query))
            }
        }
    }
}

#[derive(Clone, Default)]
pub struct CachableQueryWeighter;

impl Weighter<(SegmentId, CachableQuery), Arc<BitSet>> for CachableQueryWeighter {
    fn weight(&self, key: &(SegmentId, CachableQuery), val: &Arc<BitSet>) -> u64 {
        let bitset_size = ((val.max_value() as usize + 63) / 64) * 8;
        let key_size = std::mem::size_of::<(SegmentId, CachableQuery)>();

        let type_size = match &key.1 {
            CachableQuery::Complex(bytes) => bytes.len() + std::mem::size_of::<Box<[u8]>>(),
            CachableQuery::ByPartKey(part_key) => part_key.len() + std::mem::size_of::<Box<[u8]>>(),
            CachableQuery::ByPartIds(part_ids) => {
                (part_ids.len() * std::mem::size_of::<i32>()) + std::mem::size_of::<Box<[i32]>>()
            }
            CachableQuery::All => 0,
            CachableQuery::ByPartId(_) => 0,
            CachableQuery::ByEndTime(_) => 0,
        };

        (type_size + key_size + bitset_size) as u64
    }
}

#[cfg(test)]
mod tests {
    use tantivy::query::EmptyQuery;

    use tantivy_utils::{query::cache::CachableQuery as _, test_utils::build_test_schema};

    use super::*;

    #[test]
    fn test_should_cache() {
        assert!(CachableQuery::Complex(Arc::new([0u8; 0].into())).should_cache());
        assert!(CachableQuery::ByPartIds(Arc::new([0i32; 0].into())).should_cache());
        assert!(CachableQuery::ByEndTime(0).should_cache());
        assert!(!CachableQuery::All.should_cache());
        assert!(!CachableQuery::ByPartId(0).should_cache());
        assert!(!CachableQuery::ByPartKey(Arc::new([0u8; 0].into())).should_cache());
    }

    #[test]
    fn test_complex_query() {
        let index = build_test_schema();
        let weighter = CachableQueryWeighter;
        let reader = index.searcher.segment_readers().first().unwrap();
        let query = CachableQuery::Complex(Arc::new([1u8, 0u8].into()));

        let parsed = query.to_query(&index.schema, None).expect("Should succeed");

        assert!(parsed.is::<EmptyQuery>());

        assert_eq!(
            weighter.weight(
                &(reader.segment_id(), query),
                &Arc::new(BitSet::with_max_value(1))
            ),
            58
        );
    }

    #[test]
    fn test_partkey_query() {
        let index = build_test_schema();
        let weighter = CachableQueryWeighter;
        let reader = index.searcher.segment_readers().first().unwrap();
        let query = CachableQuery::ByPartKey(Arc::new([1u8, 0u8].into()));

        let parsed = query.to_query(&index.schema, None).expect("Should succeed");

        assert!(parsed.is::<TermQuery>());

        assert_eq!(
            weighter.weight(
                &(reader.segment_id(), query),
                &Arc::new(BitSet::with_max_value(1))
            ),
            58
        );
    }

    #[test]
    fn test_endtime_query() {
        let index = build_test_schema();
        let weighter = CachableQueryWeighter;
        let reader = index.searcher.segment_readers().first().unwrap();
        let query = CachableQuery::ByEndTime(0);

        let parsed = query.to_query(&index.schema, None).expect("Should succeed");

        assert!(parsed.is::<RangeQuery>());

        assert_eq!(
            weighter.weight(
                &(reader.segment_id(), query),
                &Arc::new(BitSet::with_max_value(1))
            ),
            40
        );
    }

    #[test]
    fn test_all_query() {
        let index = build_test_schema();
        let weighter = CachableQueryWeighter;
        let reader = index.searcher.segment_readers().first().unwrap();
        let query = CachableQuery::All;

        let parsed = query.to_query(&index.schema, None).expect("Should succeed");

        assert!(parsed.is::<AllQuery>());

        assert_eq!(
            weighter.weight(
                &(reader.segment_id(), query),
                &Arc::new(BitSet::with_max_value(1))
            ),
            40
        );
    }

    #[test]
    fn test_partid_query() {
        let index = build_test_schema();
        let weighter = CachableQueryWeighter;
        let reader = index.searcher.segment_readers().first().unwrap();
        let query = CachableQuery::ByPartId(0);

        let parsed = query.to_query(&index.schema, None).expect("Should succeed");

        assert!(parsed.is::<TermQuery>());

        assert_eq!(
            weighter.weight(
                &(reader.segment_id(), query),
                &Arc::new(BitSet::with_max_value(1))
            ),
            40
        );
    }

    #[test]
    fn test_partids_query() {
        let index = build_test_schema();
        let weighter = CachableQueryWeighter;
        let reader = index.searcher.segment_readers().first().unwrap();
        let query = CachableQuery::ByPartIds(Arc::new([1, 2].into()));

        let parsed = query.to_query(&index.schema, None).expect("Should succeed");

        assert!(parsed.is::<TermSetQuery>());

        assert_eq!(
            weighter.weight(
                &(reader.segment_id(), query),
                &Arc::new(BitSet::with_max_value(1))
            ),
            64
        );
    }
}
