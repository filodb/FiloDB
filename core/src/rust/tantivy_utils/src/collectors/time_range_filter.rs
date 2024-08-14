//! Filter collector that applies a start and end time range

use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::Column,
    TantivyError,
};

use crate::collectors::column_cache::ColumnCache;
use crate::field_constants;

use super::limited_collector::{LimitResult, LimitedCollector, LimitedSegmentCollector};

/// Filters results on a time range
pub struct TimeRangeFilter<'a, T>
where
    T: LimitedCollector,
    T::Child: LimitedSegmentCollector,
{
    /// Inner collector
    collector: &'a T,
    /// Start time
    start: i64,
    /// End time
    end: i64,
    /// Column cache
    column_cache: ColumnCache,
}

impl<'a, T> TimeRangeFilter<'a, T>
where
    T: LimitedCollector,
    T::Child: LimitedSegmentCollector,
{
    pub fn new(collector: &'a T, start: i64, end: i64, column_cache: ColumnCache) -> Self {
        Self {
            collector,
            start,
            end,
            column_cache,
        }
    }
}

impl<'a, T> LimitedCollector for TimeRangeFilter<'a, T>
where
    T: LimitedCollector,
    T::Child: LimitedSegmentCollector,
{
    fn limit(&self) -> usize {
        self.collector.limit()
    }
}

impl<'a, T> Collector for TimeRangeFilter<'a, T>
where
    T: LimitedCollector,
    T::Child: LimitedSegmentCollector,
{
    type Fruit = T::Fruit;

    type Child = TimeRangeFilterSegmentCollector<T::Child>;

    fn for_segment(
        &self,
        segment_local_id: tantivy::SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let start_column = if self.end < i64::MAX {
            Some(
                self.column_cache
                    .get_column(segment, field_constants::START_TIME)?
                    .ok_or_else(|| {
                        TantivyError::FieldNotFound(field_constants::START_TIME.to_string())
                    })?,
            )
        } else {
            None
        };

        let end_column = if self.start > 0 {
            Some(
                self.column_cache
                    .get_column(segment, field_constants::END_TIME)?
                    .ok_or_else(|| {
                        TantivyError::FieldNotFound(field_constants::END_TIME.to_string())
                    })?,
            )
        } else {
            None
        };

        let collector = self.collector.for_segment(segment_local_id, segment)?;

        Ok(TimeRangeFilterSegmentCollector::<T::Child> {
            start_column,
            end_column,
            start_time: self.start,
            end_time: self.end,
            collector,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as tantivy::collector::SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        self.collector.merge_fruits(segment_fruits)
    }
}

pub struct TimeRangeFilterSegmentCollector<T>
where
    T: LimitedSegmentCollector,
{
    collector: T,
    start_column: Option<Column<i64>>,
    end_column: Option<Column<i64>>,
    start_time: i64,
    end_time: i64,
}

impl<T> LimitedSegmentCollector for TimeRangeFilterSegmentCollector<T>
where
    T: LimitedSegmentCollector,
{
    fn collect_with_limiter(
        &mut self,
        doc: tantivy::DocId,
        score: tantivy::Score,
        limiter: Option<&mut super::limited_collector::LimitCounter>,
    ) -> LimitResult {
        if let Some(start_column) = &self.start_column {
            let doc_start = start_column.first(doc).unwrap_or(0);
            if doc_start > self.end_time {
                return Ok(());
            }
        }

        if let Some(end_column) = &self.end_column {
            let doc_end = end_column.first(doc).unwrap_or(i64::MAX);
            if doc_end < self.start_time {
                return Ok(());
            }
        }

        self.collector.collect_with_limiter(doc, score, limiter)
    }
}

impl<T> SegmentCollector for TimeRangeFilterSegmentCollector<T>
where
    T: LimitedSegmentCollector,
{
    type Fruit = T::Fruit;

    fn collect(&mut self, doc: tantivy::DocId, score: tantivy::Score) {
        let _ = self.collect_with_limiter(doc, score, None);
    }

    fn harvest(self) -> Self::Fruit {
        self.collector.harvest()
    }
}

#[cfg(test)]
mod tests {
    use tantivy::{collector::Count, query::AllQuery};

    use crate::{collectors::limited_collector::UnlimitedCollector, test_utils::build_test_schema};

    use super::*;

    #[test]
    fn test_time_filter_match_all() {
        let index = build_test_schema();
        let cache = ColumnCache::new();

        let query = AllQuery;
        let collector = UnlimitedCollector::new(Count);
        let collector = TimeRangeFilter::new(&collector, 0, i64::MAX, cache);
        let result = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Should match both docs since there's no effective time filter
        assert_eq!(result, 2);
    }

    #[test]
    fn test_time_filter_match_end_filter() {
        let index = build_test_schema();
        let cache = ColumnCache::new();

        let query = AllQuery;
        let collector = UnlimitedCollector::new(Count);
        let collector = TimeRangeFilter::new(&collector, 0, 2000, cache);
        let result = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Should match one doc since the other starts after end
        assert_eq!(result, 1);
    }

    #[test]
    fn test_time_filter_match_start_filter() {
        let index = build_test_schema();
        let cache = ColumnCache::new();

        let query = AllQuery;
        let collector = UnlimitedCollector::new(Count);
        let collector = TimeRangeFilter::new(&collector, 2000, i64::MAX, cache);
        let result = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Should match one doc since the other ends after start
        assert_eq!(result, 1);
    }

    #[test]
    fn test_time_filter_match_overlap() {
        let index = build_test_schema();
        let cache = ColumnCache::new();

        let query = AllQuery;
        let collector = UnlimitedCollector::new(Count);
        let collector = TimeRangeFilter::new(&collector, 1000, 2000, cache);
        let result = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Should match one doc since the other ends after start
        assert_eq!(result, 1);
    }

    #[test]
    fn test_time_filter_match_outside_range() {
        let index = build_test_schema();
        let cache = ColumnCache::new();

        let query = AllQuery;
        let collector = UnlimitedCollector::new(Count);
        let collector = TimeRangeFilter::new(&collector, 20_000, 20_000, cache);
        let result = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Should match no docs - out of range
        assert_eq!(result, 0);
    }
}
