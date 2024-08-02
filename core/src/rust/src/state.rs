//! State objects shared with Java

// Temporary until all logic is checked in
#![allow(dead_code)]

use std::{
    collections::{BTreeMap, HashMap},
    sync::{atomic::AtomicBool, RwLock},
};

use jni::sys::jlong;
use tantivy::{
    schema::{Field, OwnedValue, Schema},
    IndexReader, IndexWriter, TantivyDocument,
};

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
    // Are there changes pending to commit
    pub changes_pending: AtomicBool,

    // Fields that need synchronization
    //
    //
    // Active writer
    pub writer: RwLock<IndexWriter>,
}

impl IndexHandle {
    pub fn new_handle(
        schema: Schema,
        default_field: Option<Field>,
        writer: IndexWriter,
        reader: IndexReader,
    ) -> jlong {
        let obj = Box::new(Self {
            schema,
            default_field,
            writer: RwLock::new(writer),
            reader,
            changes_pending: AtomicBool::new(false),
        });

        Box::into_raw(obj) as jlong
    }

    /// Decode handle back into a reference
    pub fn get_ref_from_handle<'a>(handle: jlong) -> &'a Self {
        let ptr = handle as *const IndexHandle;

        unsafe { &*ptr }
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

pub mod field_constants {
    pub fn facet_field_name(name: &str) -> String {
        format!("{}{}", FACET_FIELD_PREFIX, name)
    }

    // These should be kept in sync with the constants in  PartKeyIndex.scala
    // as they're fields that can be directly queried via incoming filters
    // or fields that are filtered out of label lists
    pub const DOCUMENT_ID: &str = "__partIdField__";
    pub const PART_ID: &str = "__partIdDv__";
    pub const PART_KEY: &str = "__partKey__";
    pub const LABEL_LIST: &str = "__labelList__";
    pub const FACET_FIELD_PREFIX: &str = "$facet_";
    pub const START_TIME: &str = "__startTime__";
    pub const END_TIME: &str = "__endTime__";
    pub const TYPE: &str = "_type_";
}
