//! State objects shared with Java

// Temporary until all logic is checked in
#![allow(dead_code)]

use std::{
    collections::{BTreeMap, HashMap},
    sync::{atomic::AtomicBool, Mutex, RwLock},
};

use jni::{sys::jlong, JNIEnv};
use tantivy::{
    schema::{Field, OwnedValue, Schema},
    IndexReader, IndexWriter, TantivyDocument,
};

use crate::{errors::JavaResult, jnienv::JNIEnvExt};

pub struct IndexHandle {
    // Immutable fields that don't need synchronization
    //
    //
    // Schema for this nidex
    pub schema: Schema,
    // Default field for JSON searches
    pub default_field: Option<Field>,
    // Field data
    pub fields: FieldConstants,

    // Fields that need synchronization
    //
    //
    pub changes_pending: AtomicBool,
    // Active writer
    pub writer: RwLock<IndexWriter>,
    // Active reader
    pub reader: Mutex<IndexReader>,
    // Actively ingesting doc
    pub ingesting_doc: Mutex<IngestingDocument>,
}

impl IndexHandle {
    pub fn new_handle(
        schema: Schema,
        default_field: Option<Field>,
        writer: IndexWriter,
        reader: IndexReader,
        fields: FieldConstants,
    ) -> jlong {
        let obj = Box::new(Self {
            schema,
            default_field,
            writer: RwLock::new(writer),
            reader: Mutex::new(reader),
            changes_pending: AtomicBool::new(false),
            fields,
            ingesting_doc: Mutex::new(IngestingDocument::default()),
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

const PART_KEY_INDEX_RAW_CLASS: &str = "filodb/core/memstore/PartKeyIndexRaw";

// Field constants loaded from Scala classes
pub struct FieldConstants {
    pub part_id: String,
    pub part_id_dv: String,
    pub part_key: String,
    pub start_time: String,
    pub end_time: String,
    pub label_list: String,
    pub facet_field_prefix: String,
}

impl FieldConstants {
    /// Get a computed facet field name
    pub fn facet_field_name(&self, name: &str) -> String {
        format!("{}{}", self.facet_field_prefix, name)
    }

    /// Load constants from Scala runtime classes
    pub fn load(env: &mut JNIEnv) -> JavaResult<Self> {
        let constants_object = env.get_scala_object(PART_KEY_INDEX_RAW_CLASS)?;

        let part_id = env.get_scala_text_constant(&constants_object, "PART_ID_FIELD")?;
        let part_id_dv = env.get_scala_text_constant(&constants_object, "PART_ID_DV")?;
        let part_key = env.get_scala_text_constant(&constants_object, "PART_KEY")?;
        let start_time = env.get_scala_text_constant(&constants_object, "START_TIME")?;
        let end_time = env.get_scala_text_constant(&constants_object, "END_TIME")?;
        let label_list = env.get_scala_text_constant(&constants_object, "LABEL_LIST")?;
        let facet_field_prefix =
            env.get_scala_text_constant(&constants_object, "FACET_FIELD_PREFIX")?;

        Ok(Self {
            part_id,
            part_id_dv,
            part_key,
            start_time,
            end_time,
            label_list,
            facet_field_prefix,
        })
    }
}
