//! Collector that can abort early for return limits, for example

use tantivy::{
    collector::{Collector, SegmentCollector},
    query::Weight,
    DocId, Score, SegmentReader, TantivyError, TERMINATED,
};

mod limit_counter;
mod unlimited_collector;

pub use limit_counter::{LimitCounter, LimitCounterOptionExt};
pub use unlimited_collector::{UnlimitedCollector, UnlimitedSegmentCollector};

/// Marker struct for exceeding limits as an error
pub struct LimitExceeded;

pub type LimitResult = Result<(), LimitExceeded>;

/// Segment collector that can use a limiter
pub trait LimitedSegmentCollector: SegmentCollector {
    fn collect_with_limiter(
        &mut self,
        doc: DocId,
        score: Score,
        limiter: Option<&mut LimitCounter>,
    ) -> LimitResult;
}

/// A collector that can use a limiter to abort early
/// This is modelled off the Lucene behavior where you can
/// throw an exception to stop the collection.
///
/// Since Rust has no exceptions, we need an error path which
/// requires an extension trait.
pub trait LimitedCollector
where
    Self: Collector,
    Self::Child: LimitedSegmentCollector,
{
    /// Returns configured limit
    fn limit(&self) -> usize;

    fn collect_segment_with_limiter(
        &self,
        weight: &dyn Weight,
        segment_ord: u32,
        reader: &SegmentReader,
        limiter: &mut LimitCounter,
    ) -> Result<<Self::Child as SegmentCollector>::Fruit, TantivyError> {
        let mut segment_collector = self.for_segment(segment_ord, reader)?;
        let mut scorer = weight.scorer(reader, 1.0)?;

        match (reader.alive_bitset(), self.requires_scoring()) {
            (Some(alive_bitset), false) => {
                let mut doc = scorer.doc();
                while doc != TERMINATED {
                    if alive_bitset.is_alive(doc)
                        && segment_collector
                            .collect_with_limiter(doc, scorer.score(), Some(limiter))
                            .is_err()
                    {
                        // Hit limit
                        break;
                    }
                    doc = scorer.advance();
                }
            }
            (None, false) => {
                let mut doc = scorer.doc();
                while doc != TERMINATED {
                    if segment_collector
                        .collect_with_limiter(doc, scorer.score(), Some(limiter))
                        .is_err()
                    {
                        break;
                    }
                    doc = scorer.advance();
                }
            }
            (_, true) => {
                return Err(TantivyError::InvalidArgument(
                    "Scoring not supported".into(),
                ));
            }
        }

        Ok(segment_collector.harvest())
    }
}
