//! Weight adapter for a cached bitset

use std::sync::Arc;

use tantivy::{
    query::{ConstScorer, Explanation, Scorer, Weight},
    DocId, Score, SegmentReader, TantivyError,
};
use tantivy_common::BitSet;

use super::shared_doc_set::SharedDocSet;

/// Weight that can play back a cached doc set
pub struct BitSetWeight {
    bitset: Arc<BitSet>,
}

impl BitSetWeight {
    pub fn new(bitset: Arc<BitSet>) -> Self {
        BitSetWeight { bitset }
    }
}

impl Weight for BitSetWeight {
    fn scorer(&self, _reader: &SegmentReader, _boost: Score) -> tantivy::Result<Box<dyn Scorer>> {
        let docs = SharedDocSet::new(self.bitset.clone());
        Ok(Box::new(ConstScorer::new(docs, 1.0)))
    }

    fn explain(&self, reader: &SegmentReader, doc: DocId) -> tantivy::Result<Explanation> {
        let mut scorer = self.scorer(reader, 1.0)?;
        if scorer.seek(doc) == doc {
            Ok(Explanation::new("BitSetWeight", 1.0))
        } else {
            Err(TantivyError::InvalidArgument(
                "Document does not exist".to_string(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use tantivy::TERMINATED;

    use crate::test_utils::build_test_schema;

    use super::*;

    #[test]
    fn test_bitset_weight() {
        let index = build_test_schema();

        let mut bitset = BitSet::with_max_value(100);
        bitset.insert(1);
        bitset.insert(10);
        bitset.insert(100);

        let weight = BitSetWeight::new(bitset.into());
        let reader = index.searcher.segment_readers().first().unwrap();

        let mut scorer = weight.scorer(reader, 1.0).expect("Should succeed");

        assert_eq!(scorer.doc(), 1);
        scorer.advance();
        assert_eq!(scorer.doc(), 10);
        scorer.advance();
        assert_eq!(scorer.doc(), 100);
        scorer.advance();
        assert_eq!(scorer.doc(), TERMINATED);
    }

    #[test]
    fn test_bitset_explain() {
        let index = build_test_schema();

        let mut bitset = BitSet::with_max_value(100);
        bitset.insert(1);

        let weight = BitSetWeight::new(bitset.into());
        let reader = index.searcher.segment_readers().first().unwrap();

        let explanation = weight.explain(reader, 1).expect("Should succeed");

        assert_eq!(
            format!("{:?}", explanation),
            "Explanation({\n  \"value\": 1.0,\n  \"description\": \"BitSetWeight\"\n})"
        );
    }
}
