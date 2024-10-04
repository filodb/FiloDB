//! State objects shared with Java

use std::{
    collections::{BTreeMap, HashMap},
    sync::{atomic::AtomicBool, RwLock},
};

use filesize::PathExt;
use jni::sys::jlong;
use tantivy::{
    directory::{MmapDirectory, WatchCallback, WatchHandle},
    schema::{Field, OwnedValue, Schema},
    Directory, IndexReader, IndexWriter, Searcher, TantivyDocument, TantivyError,
};
use tantivy_utils::{
    collectors::{
        column_cache::ColumnCache,
        limited_collector::{LimitedCollector, LimitedSegmentCollector},
    },
    query::cache::QueryCache,
};

use crate::query_parser::filodb_query::{CachableQueryWeighter, FiloDBQuery};

pub struct IndexHandle {
    // Fields that don't need explicit synchronization
    //
    //
    // Schema for this nidex
    pub schema: Schema,
    // Default field for JSON searches
    pub default_field: Option<Field>,
    // Active reader
    pub reader: IndexReader,
    // Cache of query -> docs
    query_cache: QueryCache<FiloDBQuery, CachableQueryWeighter>,
    // Are there changes pending to commit
    pub changes_pending: AtomicBool,
    // Column lookup cache
    pub column_cache: ColumnCache,
    // Mmap dir - used for stats only
    pub mmap_directory: MmapDirectory,
    // Watch handle - notifies when to clear the column cache
    _watch_handle: WatchHandle,

    // Fields that need synchronization
    //
    //
    // Active writer
    pub writer: RwLock<IndexWriter>,
}

impl IndexHandle {
    #[allow(clippy::too_many_arguments)]
    pub fn new_handle(
        schema: Schema,
        default_field: Option<Field>,
        writer: IndexWriter,
        reader: IndexReader,
        mmap_directory: MmapDirectory,
        column_cache_size: u64,
        query_cache_max_size: u64,
        query_cache_estimated_item_size: u64,
    ) -> tantivy::Result<jlong> {
        let estimated_item_count: u64 = query_cache_max_size / query_cache_estimated_item_size;
        let column_cache = ColumnCache::new(column_cache_size as usize);

        let cache = column_cache.clone();
        // When the index segment list changes, clear the column cache to release those mmaped files
        let watch_handle = mmap_directory.watch(WatchCallback::new(move || {
            cache.clear();
        }))?;

        let obj = Box::new(Self {
            schema,
            default_field,
            writer: RwLock::new(writer),
            reader,
            changes_pending: AtomicBool::new(false),
            query_cache: QueryCache::new(estimated_item_count, query_cache_max_size),
            column_cache,
            mmap_directory,
            _watch_handle: watch_handle,
        });

        Ok(Box::into_raw(obj) as jlong)
    }

    /// Decode handle back into a reference
    pub fn get_ref_from_handle<'a>(handle: jlong) -> &'a Self {
        let ptr = handle as *const IndexHandle;

        unsafe { &*ptr }
    }

    pub fn query_cache_stats(&self) -> (u64, u64) {
        self.query_cache.query_cache_stats()
    }

    pub fn query_cache_size(&self) -> u64 {
        self.query_cache.size()
    }

    pub fn mmap_size(&self) -> u64 {
        self.mmap_directory
            .get_cache_info()
            .mmapped
            .into_iter()
            .map(|p| p.as_path().size_on_disk().unwrap_or(0))
            .sum()
    }

    pub fn searcher(&self) -> Searcher {
        self.reader.searcher()
    }

    pub fn execute_cachable_query<C>(
        &self,
        cachable_query: FiloDBQuery,
        collector: C,
    ) -> Result<C::Fruit, TantivyError>
    where
        C: LimitedCollector,
        C::Child: LimitedSegmentCollector,
    {
        let searcher = self.reader.searcher();

        self.execute_cachable_query_with_searcher(cachable_query, collector, &searcher)
    }

    pub fn execute_cachable_query_with_searcher<C>(
        &self,
        cachable_query: FiloDBQuery,
        collector: C,
        searcher: &Searcher,
    ) -> Result<C::Fruit, TantivyError>
    where
        C: LimitedCollector,
        C::Child: LimitedSegmentCollector,
    {
        self.query_cache.search(
            searcher,
            &self.schema,
            self.default_field,
            cachable_query,
            collector,
        )
    }
}

/// A document that is actively being built up for ingesting
#[derive(Default)]
pub struct IngestingDocument {
    // List of map entries we're building up to store in the document
    pub map_values: HashMap<String, BTreeMap<String, OwnedValue>>,
    // List of field names in the document being ingested
    pub field_names: Vec<String>,
    // Document state for ingestion
    pub doc: TantivyDocument,
}
