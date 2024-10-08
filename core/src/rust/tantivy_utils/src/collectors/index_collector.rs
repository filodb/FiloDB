//! Collector that can run over an entire index without a query
//!

use tantivy::{collector::SegmentCollector, Searcher, SegmentReader, TantivyError};

use super::limited_collector::{LimitCounter, LimitedCollector, LimitedSegmentCollector};

/// Index Segment collector
pub trait IndexCollector: LimitedCollector
where
    Self::Child: LimitedSegmentCollector,
{
    /// Colllect data across an entire index segment
    fn collect_over_index(
        &self,
        reader: &SegmentReader,
        limiter: &mut LimitCounter,
    ) -> Result<<Self::Child as SegmentCollector>::Fruit, TantivyError>;
}

pub fn collect_from_index<C>(searcher: &Searcher, collector: C) -> Result<C::Fruit, TantivyError>
where
    C: IndexCollector,
    C::Child: LimitedSegmentCollector,
{
    let segment_readers = searcher.segment_readers();
    let mut fruits: Vec<<C::Child as SegmentCollector>::Fruit> =
        Vec::with_capacity(segment_readers.len());

    let mut limiter = LimitCounter::new(collector.limit());

    for segment_reader in segment_readers.iter() {
        let results = collector.collect_over_index(segment_reader, &mut limiter)?;

        fruits.push(results);

        if limiter.at_limit() {
            break;
        }
    }

    collector.merge_fruits(fruits)
}
