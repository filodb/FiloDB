//! Collector to pull part IDs + time values from a document

use std::cmp::min;

use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::Column,
    TantivyError,
};

use crate::collectors::column_cache::ColumnCache;
use crate::field_constants;

use super::limited_collector::{
    LimitCounterOptionExt, LimitResult, LimitedCollector, LimitedSegmentCollector,
};

pub struct TimeCollector<'a> {
    time_field: &'a str,
    limit: usize,
    column_cache: ColumnCache,
}

impl<'a> TimeCollector<'a> {
    pub fn new(time_field: &'a str, limit: usize, column_cache: ColumnCache) -> Self {
        Self {
            time_field,
            limit,
            column_cache,
        }
    }
}

impl<'a> LimitedCollector for TimeCollector<'a> {
    fn limit(&self) -> usize {
        self.limit
    }
}

impl<'a> Collector for TimeCollector<'a> {
    // Tuple of part_id, time
    type Fruit = Vec<(i32, i64)>;

    type Child = TimeSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: tantivy::SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<TimeSegmentCollector> {
        let id_column: Column<i64> = self
            .column_cache
            .get_column(segment, field_constants::PART_ID)?
            .ok_or_else(|| TantivyError::FieldNotFound(field_constants::PART_ID.to_string()))?;

        let time_column: Column<i64> = self
            .column_cache
            .get_column(segment, self.time_field)?
            .ok_or_else(|| TantivyError::FieldNotFound(self.time_field.to_string()))?;

        Ok(TimeSegmentCollector {
            id_column,
            time_column,
            docs: Vec::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Vec<(i32, i64)>>,
    ) -> tantivy::Result<Vec<(i32, i64)>> {
        let len: usize = min(segment_fruits.iter().map(|x| x.len()).sum(), self.limit);

        let mut result = Vec::with_capacity(len);
        for part_ids in segment_fruits {
            result.extend(part_ids.iter().take(self.limit - result.len()));
        }

        Ok(result)
    }
}

pub struct TimeSegmentCollector {
    id_column: Column<i64>,
    time_column: Column<i64>,
    docs: Vec<(i32, i64)>,
}

impl LimitedSegmentCollector for TimeSegmentCollector {
    fn collect_with_limiter(
        &mut self,
        doc: tantivy::DocId,
        _score: tantivy::Score,
        mut limiter: Option<&mut super::limited_collector::LimitCounter>,
    ) -> LimitResult {
        if let Some(id) = self.id_column.first(doc) {
            if let Some(time) = self.time_column.first(doc) {
                self.docs.push((id as i32, time));
                limiter.increment()?;
            }
        }

        Ok(())
    }
}

impl SegmentCollector for TimeSegmentCollector {
    type Fruit = Vec<(i32, i64)>;

    fn collect(&mut self, doc: tantivy::DocId, score: tantivy::Score) {
        let _ = self.collect_with_limiter(doc, score, None);
    }

    fn harvest(self) -> Self::Fruit {
        self.docs
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use field_constants::START_TIME;
    use tantivy::query::AllQuery;

    use crate::test_utils::build_test_schema;

    use super::*;

    #[test]
    fn test_time_collector() {
        let index = build_test_schema();
        let column_cache = ColumnCache::new();

        let collector = TimeCollector::new(START_TIME, usize::MAX, column_cache);
        let query = AllQuery;

        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Two docs, IDs 1 and 10
        assert_eq!(
            results.into_iter().collect::<HashSet<(i32, i64)>>(),
            [(1, 1234), (10, 4321)]
                .into_iter()
                .collect::<HashSet<(i32, i64)>>()
        );
    }

    #[test]
    fn test_part_id_collector_with_limit() {
        let index = build_test_schema();
        let column_cache = ColumnCache::new();

        let collector = TimeCollector::new(START_TIME, 1, column_cache);
        let query = AllQuery;

        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Which doc matches first is non deterministic, just check length
        assert_eq!(results.len(), 1);
    }
}
