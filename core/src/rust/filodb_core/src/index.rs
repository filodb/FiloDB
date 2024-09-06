//! Methods to create / destroy the index

use jni::{
    objects::{JClass, JObjectArray, JString},
    sys::{jfloat, jlong},
    JNIEnv,
};
use tantivy::{
    directory::MmapDirectory,
    indexer::LogMergePolicy,
    schema::{
        BytesOptions, FacetOptions, Field, JsonObjectOptions, NumericOptions, Schema,
        SchemaBuilder, TextFieldIndexing, TextOptions,
    },
    IndexBuilder, IndexSettings, ReloadPolicy, TantivyDocument,
};
use tantivy_utils::field_constants::{self, facet_field_name, LABEL_LIST};

use crate::{
    errors::{JavaException, JavaResult},
    exec::jni_exec,
    jnienv::JNIEnvExt,
    state::IndexHandle,
};

pub const WRITER_MEM_BUDGET: usize = 50 * 1024 * 1024;

/// Create a new index state object by loading and configuring schema
#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_newIndexHandle(
    mut env: JNIEnv,
    _class: JClass,
    disk_location: JString,
    schema_fields: JObjectArray,
    map_fields: JObjectArray,
    multi_column_facet_fields: JObjectArray,
    column_cache_size: jlong,
    query_cache_max_size: jlong,
    query_cache_estimated_item_size: jlong,
    deleted_doc_merge_threshold: jfloat,
) -> jlong {
    jni_exec(&mut env, |env| {
        let disk_location: String = env.get_string(&disk_location)?.into();
        std::fs::create_dir_all(&disk_location)?;

        let directory = MmapDirectory::open(disk_location)?;

        // Build the schema for documents
        let (schema, default_field) =
            build_schema(env, &schema_fields, &map_fields, &multi_column_facet_fields)?;

        // Open index
        let settings = IndexSettings {
            ..Default::default()
        };

        let index = IndexBuilder::new()
            .schema(schema.clone())
            .settings(settings)
            .open_or_create(directory.clone())?;

        let writer = index.writer::<TantivyDocument>(WRITER_MEM_BUDGET)?;

        let mut merge_policy = LogMergePolicy::default();
        merge_policy.set_del_docs_ratio_before_merge(deleted_doc_merge_threshold);

        writer.set_merge_policy(Box::new(merge_policy));

        let reader = index
            .reader_builder()
            // It's tempting to use Manual here as we call refresh periodically
            // from a timer thread.  However, refresh just means that you can see
            // all uncommitted documents, not that all merges have completed.  This
            // means that background merges that are happening that could speed up
            // queries aren't avaialble when manual is used.  Instead we use
            // on commit - the cost of this is minor since it's a FS notification
            // and reloading the segment list is fairly cheap and infrequent.
            .reload_policy(ReloadPolicy::OnCommitWithDelay)
            .try_into()?;

        Ok(IndexHandle::new_handle(
            schema,
            default_field,
            writer,
            reader,
            directory,
            column_cache_size as u64,
            query_cache_max_size as u64,
            query_cache_estimated_item_size as u64,
        ))
    })
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_freeIndexHandle(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    jni_exec(&mut env, |_| {
        unsafe {
            drop(Box::from_raw(handle as *mut IndexHandle));
        }

        Ok(())
    });
}

fn build_schema(
    env: &mut JNIEnv,
    schema_fields: &JObjectArray,
    map_fields: &JObjectArray,
    multi_column_facet_fields: &JObjectArray,
) -> JavaResult<(Schema, Option<Field>)> {
    let mut builder = SchemaBuilder::new();

    let text_options = TextOptions::default().set_indexing_options(
        TextFieldIndexing::default()
            .set_tokenizer("raw")
            .set_fieldnorms(false),
    );

    let random_access_text_options = text_options.clone().set_fast(Some("raw"));

    let numeric_options = NumericOptions::default().set_indexed().set_fast();

    // Bytes values are faster to read via the doc store vs fast fields and we don't need any of the fast
    // field only features like iterating by sorted values
    let byte_options = BytesOptions::default().set_indexed().set_stored();

    builder.add_text_field(field_constants::DOCUMENT_ID, text_options.clone());
    builder.add_i64_field(field_constants::PART_ID, numeric_options.clone());
    builder.add_bytes_field(field_constants::PART_KEY, byte_options);
    builder.add_i64_field(field_constants::START_TIME, numeric_options.clone());
    builder.add_i64_field(field_constants::END_TIME, numeric_options.clone());
    builder.add_text_field(field_constants::TYPE, text_options.clone());

    // Fields from input schema
    env.foreach_string_in_array(schema_fields, |name| {
        builder.add_text_field(&name, random_access_text_options.clone());

        Ok(())
    })?;

    // Map fields - only one supported
    let len = env.get_array_length(map_fields)?;
    if len > 1 {
        return Err(JavaException::new_runtime_exception(
            "More than one map field specified",
        ));
    }

    let default_field = if len == 1 {
        let name = env.get_object_array_element(map_fields, 0)?.into();
        let name = env.get_rust_string(&name)?;

        let field = builder.add_json_field(
            &name,
            JsonObjectOptions::default()
                .set_indexing_options(
                    TextFieldIndexing::default()
                        .set_tokenizer("raw")
                        .set_fieldnorms(false),
                )
                .set_fast(Some("raw")),
        );

        Some(field)
    } else {
        None
    };

    env.foreach_string_in_array(multi_column_facet_fields, |name| {
        builder.add_text_field(&name, random_access_text_options.clone());

        Ok(())
    })?;

    // Default facet for label list, always added
    builder.add_facet_field(&facet_field_name(LABEL_LIST), FacetOptions::default());

    Ok((builder.build(), default_field))
}
