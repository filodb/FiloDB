//! Methods to create / destroy the index

use jni::{
    objects::{JClass, JObjectArray, JString},
    sys::jlong,
    JNIEnv,
};
use tantivy::{
    directory::MmapDirectory,
    schema::{
        FacetOptions, Field, JsonObjectOptions, Schema, SchemaBuilder, TextFieldIndexing, FAST,
        INDEXED, STORED, STRING,
    },
    Index, TantivyDocument,
};

use crate::{
    errors::{JavaException, JavaResult},
    exec::jni_exec,
    jnienv::JNIEnvExt,
    state::{FieldConstants, IndexHandle},
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
) -> jlong {
    jni_exec(&mut env, |env| {
        let disk_location: String = env.get_string(&disk_location)?.into();
        let directory = MmapDirectory::open(disk_location)?;

        // Build the schema for documents
        let fields = FieldConstants::load(env)?;
        let (schema, default_field) = build_schema(
            env,
            &schema_fields,
            &map_fields,
            &multi_column_facet_fields,
            &fields,
        )?;

        // Open index
        let index = Index::open_or_create(directory, schema.clone())?;
        let writer = index.writer::<TantivyDocument>(WRITER_MEM_BUDGET)?;
        let reader = index.reader()?;

        Ok(IndexHandle::new_handle(
            schema,
            default_field,
            writer,
            reader,
            fields,
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
    constants: &FieldConstants,
) -> JavaResult<(Schema, Option<Field>)> {
    let mut builder = SchemaBuilder::new();

    builder.add_text_field(&constants.part_id, STRING);
    builder.add_i64_field(&constants.part_id_dv, INDEXED | STORED | FAST);
    builder.add_bytes_field(&constants.part_key, INDEXED | STORED | FAST);
    builder.add_i64_field(&constants.start_time, INDEXED | STORED | FAST);
    builder.add_i64_field(&constants.end_time, INDEXED | STORED | FAST);

    // Fields from input schema
    env.foreach_string_in_array(schema_fields, |name| {
        builder.add_text_field(&name, STRING);

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
                .set_indexing_options(TextFieldIndexing::default().set_tokenizer("raw")),
        );

        Some(field)
    } else {
        None
    };

    env.foreach_string_in_array(multi_column_facet_fields, |name| {
        builder.add_facet_field(&constants.facet_field_name(&name), FacetOptions::default());

        Ok(())
    })?;

    // Default facet for label list, always added
    builder.add_facet_field(
        &constants.facet_field_name(&constants.label_list),
        FacetOptions::default(),
    );

    Ok((builder.build(), default_field))
}
