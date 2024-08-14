//! Low memcpy sharable docset

use std::sync::Arc;

use tantivy::{DocId, DocSet, TERMINATED};
use tantivy_common::{BitSet, TinySet};

/// Allows for efficient copying of docsets from an immutable bitset
/// This is doing the same job as BitSetDocSet, but without the memcpy
/// each time we want to create a new instance
pub struct SharedDocSet {
    bits: Arc<BitSet>,
    current_word_num: u32,
    current_word: TinySet,
    current_doc: DocId,
}

impl SharedDocSet {
    pub fn new(bits: Arc<BitSet>) -> Self {
        let current_word = if bits.max_value() == 0 {
            TinySet::empty()
        } else {
            bits.tinyset(0)
        };

        let mut ret = Self {
            bits,
            current_word_num: 0,
            current_word,
            current_doc: 0,
        };

        ret.advance();
        ret
    }

    #[inline]
    fn word_count(&self) -> u32 {
        (self.bits.max_value() + 63) / 64
    }
}

impl DocSet for SharedDocSet {
    #[inline]
    fn advance(&mut self) -> DocId {
        // Case 1 - bits still in the current word
        if let Some(bit) = self.current_word.pop_lowest() {
            self.current_doc = (self.current_word_num * 64) + bit;

            // Case 2 - no more words
        } else if (self.current_word_num + 1) >= self.word_count() {
            self.current_doc = TERMINATED;

            // Case 3 - advance to next word
        } else if let Some(word_num) = self.bits.first_non_empty_bucket(self.current_word_num + 1) {
            self.current_word_num = word_num;
            self.current_word = self.bits.tinyset(word_num);

            // This is safe because first_non_empty bucket ensured it is non-empty
            #[allow(clippy::unwrap_used)]
            let bit = self.current_word.pop_lowest().unwrap();
            self.current_doc = (self.current_word_num * 64) + bit;

            // Case 4 - end of set
        } else {
            self.current_doc = TERMINATED;
        }

        self.current_doc
    }

    fn doc(&self) -> DocId {
        self.current_doc
    }

    fn seek(&mut self, target: DocId) -> DocId {
        if target >= self.bits.max_value() {
            self.current_doc = TERMINATED
        }

        let target_word = target / 64;
        if target_word > self.current_word_num {
            self.current_word_num = target_word;
            self.current_word = self.bits.tinyset(self.current_word_num);

            self.current_word = self
                .current_word
                .intersect(TinySet::range_greater_or_equal(target_word));
            self.advance();
        } else {
            while self.current_doc < target {
                self.advance();
            }
        }

        self.current_doc
    }

    fn size_hint(&self) -> u32 {
        self.bits.len() as u32
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_docset() {
        let bits = BitSet::with_max_value(0);
        let mut docset = SharedDocSet::new(bits.into());

        assert_eq!(docset.size_hint(), 0);
        assert_eq!(docset.doc(), TERMINATED);
        assert_eq!(docset.advance(), TERMINATED);
        assert_eq!(docset.seek(0), TERMINATED);
    }

    #[test]
    fn test_full_docset() {
        let bits = BitSet::with_max_value_and_full(1000);
        let mut docset = SharedDocSet::new(bits.into());

        assert_eq!(docset.size_hint(), 1000);
        for i in 0..1000 {
            assert_eq!(i as DocId, docset.doc());
            docset.advance();
        }

        assert_eq!(docset.doc(), TERMINATED);
    }

    #[test]
    fn test_full_docset_seek() {
        let bits = BitSet::with_max_value_and_full(1000);
        let mut docset = SharedDocSet::new(bits.into());

        assert_eq!(docset.size_hint(), 1000);
        docset.seek(50);
        for i in 50..1000 {
            assert_eq!(i as DocId, docset.doc());
            docset.advance();
        }

        assert_eq!(docset.doc(), TERMINATED);
    }

    #[test]
    fn test_sparse_docset() {
        let mut bits = BitSet::with_max_value(1000);
        bits.insert(100);
        bits.insert(235);
        let mut docset = SharedDocSet::new(bits.into());

        assert_eq!(docset.size_hint(), 2);
        assert_eq!(docset.doc(), 100);
        docset.advance();
        assert_eq!(docset.doc(), 235);
        docset.advance();
        assert_eq!(docset.doc(), TERMINATED);
    }

    #[test]
    fn test_sparse_docset_seek() {
        let mut bits = BitSet::with_max_value(1000);
        bits.insert(100);
        bits.insert(235);
        let mut docset = SharedDocSet::new(bits.into());

        assert_eq!(docset.size_hint(), 2);
        docset.seek(101);
        assert_eq!(docset.doc(), 235);
        docset.advance();
        assert_eq!(docset.doc(), TERMINATED);
    }
}
