//! Collector for part key binary data

use std::cmp::min;

use crate::collectors::column_cache::ColumnCache;
use crate::field_constants::{END_TIME, PART_KEY, START_TIME};
use tantivy::schema::OwnedValue;
use tantivy::{
    collector::{Collector, SegmentCollector},
    columnar::Column,
    TantivyError,
};
use tantivy::{DocAddress, Searcher, TantivyDocument};

use super::limited_collector::{
    LimitCounterOptionExt, LimitResult, LimitedCollector, LimitedSegmentCollector,
};

/// Records returned from queries
#[derive(Debug, PartialEq, Hash, PartialOrd, Eq)]
pub struct UnresolvedPartKeyRecord {
    pub doc_id: DocAddress,
    pub start_time: i64,
    pub end_time: i64,
}

impl UnresolvedPartKeyRecord {
    pub fn resolve(self, searcher: &Searcher) -> Result<PartKeyRecord, TantivyError> {
        let doc_data = searcher.doc::<TantivyDocument>(self.doc_id)?;
        let part_key_field = searcher.schema().get_field(PART_KEY)?;

        let Some(OwnedValue::Bytes(part_key)) = doc_data
            .into_iter()
            .filter(|x| x.field == part_key_field)
            .map(|x| x.value)
            .next()
        else {
            return Err(TantivyError::FieldNotFound(PART_KEY.to_string()));
        };

        Ok(PartKeyRecord {
            part_key,
            start_time: self.start_time,
            end_time: self.end_time,
        })
    }
}

/// Records returned from queries
#[derive(Debug, PartialEq, Hash, PartialOrd, Eq)]
pub struct PartKeyRecord {
    pub part_key: Vec<u8>,
    pub start_time: i64,
    pub end_time: i64,
}

impl PartKeyRecord {
    pub fn serialized_len(&self) -> usize {
        // Two i64 ints, 1 u32 int, byte array
        self.part_key.len() + size_of::<u32>() + (2 * size_of::<i64>())
    }

    pub fn serialize(self, vec: &mut impl Extend<u8>) {
        // Serialize as start time, end time, part_key len, part_key
        vec.extend(self.start_time.to_le_bytes());
        vec.extend(self.end_time.to_le_bytes());

        let len = self.part_key.len() as u32;
        vec.extend(len.to_le_bytes());
        vec.extend(self.part_key);
    }
}

pub struct PartKeyRecordCollector {
    limit: usize,
    column_cache: ColumnCache,
}

impl PartKeyRecordCollector {
    pub fn new(limit: usize, column_cache: ColumnCache) -> Self {
        Self {
            limit,
            column_cache,
        }
    }
}

impl LimitedCollector for PartKeyRecordCollector {
    fn limit(&self) -> usize {
        self.limit
    }
}

impl Collector for PartKeyRecordCollector {
    type Fruit = Vec<UnresolvedPartKeyRecord>;

    type Child = PartKeyRecordSegmentCollector;

    fn for_segment(
        &self,
        segment_local_id: tantivy::SegmentOrdinal,
        segment: &tantivy::SegmentReader,
    ) -> tantivy::Result<PartKeyRecordSegmentCollector> {
        let start_time_column: Column<i64> = self
            .column_cache
            .get_column(segment, START_TIME)?
            .ok_or_else(|| TantivyError::FieldNotFound(START_TIME.to_string()))?;

        let end_time_column: Column<i64> = self
            .column_cache
            .get_column(segment, END_TIME)?
            .ok_or_else(|| TantivyError::FieldNotFound(END_TIME.to_string()))?;

        Ok(PartKeyRecordSegmentCollector {
            segment_ord: segment_local_id,
            start_time_column,
            end_time_column,
            docs: Vec::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Vec<UnresolvedPartKeyRecord>>,
    ) -> tantivy::Result<Vec<UnresolvedPartKeyRecord>> {
        let len: usize = min(segment_fruits.iter().map(|x| x.len()).sum(), self.limit);

        let mut result = Vec::with_capacity(len);
        for part_ids in segment_fruits {
            result.extend(part_ids.into_iter().take(self.limit - result.len()));
        }

        Ok(result)
    }
}

pub struct PartKeyRecordSegmentCollector {
    segment_ord: u32,
    start_time_column: Column<i64>,
    end_time_column: Column<i64>,
    docs: Vec<UnresolvedPartKeyRecord>,
}

impl LimitedSegmentCollector for PartKeyRecordSegmentCollector {
    fn collect_with_limiter(
        &mut self,
        doc: tantivy::DocId,
        _score: tantivy::Score,
        mut limiter: Option<&mut super::limited_collector::LimitCounter>,
    ) -> LimitResult {
        let doc_id = DocAddress::new(self.segment_ord, doc);

        let Some(start_time) = self.start_time_column.first(doc) else {
            return Ok(());
        };

        let Some(end_time) = self.end_time_column.first(doc) else {
            return Ok(());
        };

        self.docs.push(UnresolvedPartKeyRecord {
            doc_id,
            start_time,
            end_time,
        });

        limiter.increment()?;

        Ok(())
    }
}

impl SegmentCollector for PartKeyRecordSegmentCollector {
    type Fruit = Vec<UnresolvedPartKeyRecord>;

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

    use tantivy::query::AllQuery;

    use crate::test_utils::build_test_schema;

    use super::*;

    #[test]
    fn test_part_key_collector() {
        let index = build_test_schema();
        let column_cache = ColumnCache::default();

        let collector = PartKeyRecordCollector::new(usize::MAX, column_cache);
        let query = AllQuery;

        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Two docs, IDs 1 and 10
        assert_eq!(
            results
                .into_iter()
                .map(|x| x.resolve(&index.searcher).unwrap())
                .collect::<HashSet<_>>(),
            [
                PartKeyRecord {
                    part_key: vec![0x41, 0x41],
                    start_time: 1234,
                    end_time: 1235
                },
                PartKeyRecord {
                    part_key: vec![0x42, 0x42],
                    start_time: 4321,
                    end_time: 10000
                }
            ]
            .into_iter()
            .collect::<HashSet<_>>()
        );
    }

    #[test]
    fn test_part_key_collector_with_limit() {
        let index = build_test_schema();
        let column_cache = ColumnCache::default();

        let collector = PartKeyRecordCollector::new(1, column_cache);
        let query = AllQuery;

        let results = index
            .searcher
            .search(&query, &collector)
            .expect("Should succeed");

        // Which doc matches first is non deterministic, just check length
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_part_key_record_serialize() {
        let record = PartKeyRecord {
            part_key: vec![0xAAu8; 2],
            start_time: 1,
            end_time: 2,
        };

        // 8 bytes, 8 bytes, 4 bytes, 2 bytes
        assert_eq!(22, record.serialized_len());

        let mut vec = vec![];
        record.serialize(&mut vec);

        assert_eq!(
            vec![1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 170, 170],
            vec
        );
    }
}
