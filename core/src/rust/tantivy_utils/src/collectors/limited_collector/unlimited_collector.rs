//! Helper to wrap collectors as unlimited
//! This is needed for collectors outside this crate, such as the
//! built-in Tantivy ones

use tantivy::{
    collector::{Collector, SegmentCollector},
    DocId, Score, SegmentReader,
};

use super::{LimitCounter, LimitResult, LimitedCollector, LimitedSegmentCollector};

/// Wrapper to adapt not limited collectors into the limiting framework
pub struct UnlimitedCollector<T>
where
    T: Collector,
{
    inner: T,
}

impl<T> UnlimitedCollector<T>
where
    T: Collector,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> LimitedCollector for UnlimitedCollector<T>
where
    T: Collector,
{
    fn limit(&self) -> usize {
        usize::MAX
    }
}

impl<T> Collector for UnlimitedCollector<T>
where
    T: Collector,
{
    type Fruit = T::Fruit;

    type Child = UnlimitedSegmentCollector<T::Child>;

    fn for_segment(
        &self,
        segment_local_id: tantivy::SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let segment_collector = self.inner.for_segment(segment_local_id, segment)?;

        Ok(UnlimitedSegmentCollector::new(segment_collector))
    }

    fn requires_scoring(&self) -> bool {
        self.inner.requires_scoring()
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        self.inner.merge_fruits(segment_fruits)
    }
}

pub struct UnlimitedSegmentCollector<T>
where
    T: SegmentCollector,
{
    inner: T,
}

impl<T> UnlimitedSegmentCollector<T>
where
    T: SegmentCollector,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> LimitedSegmentCollector for UnlimitedSegmentCollector<T>
where
    T: SegmentCollector,
{
    fn collect_with_limiter(
        &mut self,
        doc: DocId,
        score: Score,
        _limiter: Option<&mut LimitCounter>,
    ) -> LimitResult {
        self.inner.collect(doc, score);

        Ok(())
    }
}

impl<T> SegmentCollector for UnlimitedSegmentCollector<T>
where
    T: SegmentCollector,
{
    type Fruit = T::Fruit;

    fn collect(&mut self, doc: DocId, score: Score) {
        self.inner.collect(doc, score)
    }

    fn harvest(self) -> Self::Fruit {
        self.inner.harvest()
    }
}
