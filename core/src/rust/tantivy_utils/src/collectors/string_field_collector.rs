//! Collector to string values from a document

use std::collections::hash_map::Entry;

use hashbrown::HashMap;
use nohash_hasher::IntMap;
use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::StrColumn,
};

use crate::collectors::column_cache::ColumnCache;

use super::limited_collector::{
    LimitCounterOptionExt, LimitResult, LimitedCollector, LimitedSegmentCollector,
};

pub struct StringFieldCollector<'a> {
    field: &'a str,
    limit: usize,
    term_limit: usize,
    column_cache: ColumnCache,
}

impl<'a> StringFieldCollector<'a> {
    pub fn new(field: &'a str, limit: usize, term_limit: usize, column_cache: ColumnCache) -> Self {
        Self {
            field,
            limit,
            term_limit,
            column_cache,
        }
    }
}

impl<'a> LimitedCollector for StringFieldCollector<'a> {
    fn limit(&self) -> usize {
        self.limit
    }
}

impl<'a> Collector for StringFieldCollector<'a> {
    type Fruit = Vec<(String, u64)>;

    type Child = StringFieldSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: tantivy::SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<StringFieldSegmentCollector> {
        let column = self.column_cache.get_str_column(segment, self.field)?;

        Ok(StringFieldSegmentCollector {
            column,
            docs: IntMap::default(),
            term_limit: self.term_limit,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<HashMap<String, u64>>,
    ) -> tantivy::Result<Vec<(String, u64)>> {
        let mut results = HashMap::new();

        for mut map in segment_fruits.into_iter() {
            for (value, count) in map.drain() {
                *results.entry(value).or_insert(0) += count;
            }
        }

        let mut results: Vec<_> = results.drain().collect();
        results.sort_by(|(_, count_a), (_, count_b)| count_b.cmp(count_a));

        let results = results.into_iter().take(self.limit).collect();

        Ok(results)
    }
}

pub struct StringFieldSegmentCollector {
    column: Option<StrColumn>,
    docs: IntMap<u64, u64>,
    term_limit: usize,
}

impl LimitedSegmentCollector for StringFieldSegmentCollector {
    fn collect_with_limiter(
        &mut self,
        doc: tantivy::DocId,
        _score: tantivy::Score,
        mut limiter: Option<&mut super::limited_collector::LimitCounter>,
    ) -> LimitResult {
        if let Some(column) = &self.column {
            for ord in column.term_ords(doc) {
                if self.docs.len() >= self.term_limit {
                    break;
                }

                // We wait to translate to strings later to reduce
                // the number of times we have to copy the data out
                // to one per ord
                let entry = self.docs.entry(ord);
                let increment = matches!(entry, Entry::Vacant(_));
                *entry.or_insert(0) += 1;

                if increment {
                    limiter.increment()?;
                }
            }
        }

        Ok(())
    }
}

impl SegmentCollector for StringFieldSegmentCollector {
    type Fruit = HashMap<String, u64>;

    fn collect(&mut self, doc: tantivy::DocId, score: tantivy::Score) {
        let _ = self.collect_with_limiter(doc, score, None);
    }

    fn harvest(self) -> Self::Fruit {
        self.docs
            .into_iter()
            .map(|(ord, count)| {
                if let Some(column) = &self.column {
                    let mut value = String::new();
                    let _ = column.ord_to_str(ord, &mut value);

                    (value, count)
                } else {
                    (String::new(), count)
                }
            })
            .filter(|(k, _v)| !k.is_empty())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use tantivy::query::AllQuery;

    use crate::test_utils::{build_test_schema, COL1_NAME, JSON_COL_NAME};

    use super::*;

    #[test]
    fn test_string_field_collector() {
        let index = build_test_schema();
        let column_cache = ColumnCache::default();

        let collector = StringFieldCollector::new(COL1_NAME, usize::MAX, usize::MAX, column_cache);
        let query = AllQuery;

        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Two docs
        assert_eq!(
            results.into_iter().collect::<HashSet<_>>(),
            [("ABC".to_string(), 1), ("DEF".to_string(), 1)]
                .into_iter()
                .collect::<HashSet<_>>()
        );
    }

    #[test]
    fn test_string_field_collector_json() {
        let index = build_test_schema();
        let column_cache = ColumnCache::default();

        let col_name = format!("{}.{}", JSON_COL_NAME, "f1");
        let collector = StringFieldCollector::new(&col_name, usize::MAX, usize::MAX, column_cache);
        let query = AllQuery;

        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Two docs
        assert_eq!(
            results.into_iter().collect::<HashSet<_>>(),
            [("value".to_string(), 1), ("othervalue".to_string(), 1)]
                .into_iter()
                .collect::<HashSet<_>>()
        );
    }

    #[test]
    fn test_string_field_collector_json_invalid_field() {
        let index = build_test_schema();
        let column_cache = ColumnCache::default();

        let col_name = format!("{}.{}", JSON_COL_NAME, "invalid");
        let collector = StringFieldCollector::new(&col_name, usize::MAX, usize::MAX, column_cache);
        let query = AllQuery;

        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // No results, no failure
        assert_eq!(
            results.into_iter().collect::<HashSet<_>>(),
            [].into_iter().collect::<HashSet<_>>()
        );
    }

    #[test]
    fn test_string_field_collector_with_limit() {
        let index = build_test_schema();
        let column_cache = ColumnCache::default();

        let collector = StringFieldCollector::new(COL1_NAME, 1, usize::MAX, column_cache);
        let query = AllQuery;

        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Which doc matches first is non deterministic, just check length
        assert_eq!(results.len(), 1);
    }
}
