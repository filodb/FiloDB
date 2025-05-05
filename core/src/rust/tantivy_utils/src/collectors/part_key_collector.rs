//! Collector for part key binary data

use tantivy::{
    collector::{Collector, SegmentCollector},
    schema::OwnedValue,
    DocAddress, Searcher, TantivyDocument, TantivyError,
};

use crate::field_constants::PART_KEY;

use super::limited_collector::{LimitResult, LimitedCollector, LimitedSegmentCollector};

#[derive(Default)]
pub struct PartKeyCollector;

pub struct UnresolvedPartKey(DocAddress);

impl UnresolvedPartKey {
    pub fn resolve(self, searcher: &Searcher) -> Result<Vec<u8>, TantivyError> {
        let doc_data = searcher.doc::<TantivyDocument>(self.0)?;
        let part_key_field = searcher.schema().get_field(PART_KEY)?;

        let Some(OwnedValue::Bytes(part_key)) = doc_data
            .field_values()
            .into_iter()
            .filter(|(field, _)| *field == part_key_field)
            .map(|(_, value)| value.into())
            .next()
        else {
            return Err(TantivyError::FieldNotFound(PART_KEY.to_string()));
        };

        Ok(part_key)
    }
}

impl PartKeyCollector {
    pub fn new() -> Self {
        Self {}
    }
}

impl LimitedCollector for PartKeyCollector {
    fn limit(&self) -> usize {
        usize::MAX
    }
}

impl Collector for PartKeyCollector {
    type Fruit = Option<UnresolvedPartKey>;

    type Child = PartKeySegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: tantivy::SegmentOrdinal,
        _segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<PartKeySegmentCollector> {
        Ok(PartKeySegmentCollector {
            segment_local_id,
            doc: None,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Option<UnresolvedPartKey>>,
    ) -> tantivy::Result<Option<UnresolvedPartKey>> {
        Ok(segment_fruits.into_iter().flatten().next())
    }
}

pub struct PartKeySegmentCollector {
    segment_local_id: u32,
    doc: Option<UnresolvedPartKey>,
}

impl LimitedSegmentCollector for PartKeySegmentCollector {
    fn collect_with_limiter(
        &mut self,
        doc: tantivy::DocId,
        score: tantivy::Score,
        _limiter: Option<&mut super::limited_collector::LimitCounter>,
    ) -> LimitResult {
        self.collect(doc, score);

        Ok(())
    }
}

impl SegmentCollector for PartKeySegmentCollector {
    type Fruit = Option<UnresolvedPartKey>;

    fn collect(&mut self, doc: tantivy::DocId, _score: tantivy::Score) {
        if self.doc.is_some() {
            return;
        }

        self.doc = Some(UnresolvedPartKey(DocAddress::new(
            self.segment_local_id,
            doc,
        )));
    }

    fn harvest(self) -> Self::Fruit {
        self.doc
    }
}

#[cfg(test)]
mod tests {
    use tantivy::{
        query::{EmptyQuery, TermQuery},
        schema::IndexRecordOption,
        Term,
    };

    use crate::test_utils::{build_test_schema, COL1_NAME};

    use super::*;

    #[test]
    fn test_part_key_collector() {
        let index = build_test_schema();

        let collector = PartKeyCollector::new();
        let query = TermQuery::new(
            Term::from_field_text(index.schema.get_field(COL1_NAME).unwrap(), "ABC"),
            IndexRecordOption::Basic,
        );

        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        assert_eq!(
            results.map(|r| r.resolve(&index.searcher).unwrap()),
            Some(vec![0x41, 0x41])
        );
    }

    #[test]
    fn test_part_key_collector_no_match() {
        let index = build_test_schema();

        let collector = PartKeyCollector::new();

        let query = EmptyQuery;

        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Which doc matches first is non deterministic, just check length
        assert_eq!(results.map(|r| r.resolve(&index.searcher).unwrap()), None);
    }
}
