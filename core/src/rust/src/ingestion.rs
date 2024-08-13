//! Methods that modify the index / do data ingestion

use std::{ops::Bound, sync::atomic::Ordering};

use fields::add_fields;
use jni::{
    objects::{JByteArray, JClass, JIntArray, JString},
    sys::{jboolean, jint, jlong, JNI_TRUE},
    JNIEnv,
};
use tantivy::{
    collector::Count,
    indexer::UserOperation,
    query::{RangeQuery, TermSetQuery},
    schema::Facet,
    TantivyDocument, Term,
};

use crate::{
    errors::JavaResult,
    exec::jni_exec,
    jnienv::JNIEnvExt,
    state::{
        field_constants::{self, facet_field_name},
        IndexHandle, IngestingDocument,
    },
};

mod fields;

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_reset(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    jni_exec(&mut env, |_| {
        let handle = IndexHandle::get_ref_from_handle(handle);

        handle.changes_pending.store(false, Ordering::SeqCst);

        let mut writer = handle.writer.write()?;
        writer.delete_all_documents()?;
        writer.commit()?;

        handle.changes_pending.store(false, Ordering::SeqCst);

        Ok(())
    });
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_commit(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) {
    jni_exec(&mut env, |_| {
        let handle = IndexHandle::get_ref_from_handle(handle);

        handle.changes_pending.store(false, Ordering::SeqCst);

        let mut writer = handle.writer.write()?;
        writer.commit()?;

        Ok(())
    });
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_ingestDocument(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    part_key_data: JByteArray,
    part_key_offset: jint,
    part_key_num_bytes: jint,
    part_id: jint,
    document_id: JString,
    start_time: jlong,
    end_time: jlong,
    fields: JByteArray,
    upsert: jboolean,
) {
    jni_exec(&mut env, |env| {
        let handle = IndexHandle::get_ref_from_handle(handle);

        let mut ingesting_doc = IngestingDocument::default();

        if part_id > -1 {
            ingesting_doc.doc.add_i64(
                handle.schema.get_field(field_constants::PART_ID)?,
                part_id.into(),
            );
        }

        let document_id = env.get_rust_string(&document_id)?;
        ingesting_doc.doc.add_text(
            handle.schema.get_field(field_constants::DOCUMENT_ID)?,
            document_id.clone(),
        );

        ingesting_doc.doc.add_i64(
            handle.schema.get_field(field_constants::START_TIME)?,
            start_time,
        );

        ingesting_doc.doc.add_i64(
            handle.schema.get_field(field_constants::END_TIME)?,
            end_time,
        );

        let bytes = env.get_byte_array_offset_len(
            &part_key_data,
            part_key_offset as usize,
            part_key_num_bytes as usize,
        )?;

        ingesting_doc
            .doc
            .add_bytes(handle.schema.get_field(field_constants::PART_KEY)?, bytes);

        // Add dynamic fields
        let fields = env.get_byte_array(&fields)?;
        add_fields(&fields, &mut ingesting_doc, &handle.schema)?;

        let doc = prepare_tantivy_doc(handle, &mut ingesting_doc)?;

        // Save it
        let writer = handle.writer.read()?;

        if upsert == JNI_TRUE {
            let delete_term = Term::from_field_text(
                handle.schema.get_field(field_constants::DOCUMENT_ID)?,
                &document_id,
            );

            let writer = handle.writer.read()?;
            writer.run([UserOperation::Delete(delete_term), UserOperation::Add(doc)])?;

            handle.changes_pending.store(true, Ordering::SeqCst);
        } else {
            writer.add_document(doc)?;
        }

        handle.changes_pending.store(true, Ordering::SeqCst);

        Ok(())
    });
}

fn prepare_tantivy_doc(
    handle: &IndexHandle,
    ingesting_doc: &mut IngestingDocument,
) -> JavaResult<TantivyDocument> {
    let mut map_values = std::mem::take(&mut ingesting_doc.map_values);

    // Insert map columns we've built up
    for (key, value) in map_values.drain() {
        ingesting_doc
            .doc
            .add_object(handle.schema.get_field(&key)?, value);
    }

    // Build final facet for field list
    let mut field_names = std::mem::take(&mut ingesting_doc.field_names);
    field_names.sort();

    for field in field_names {
        add_facet(
            handle,
            ingesting_doc,
            field_constants::LABEL_LIST,
            &[field.as_str()],
        )?;
    }

    let doc = std::mem::take(&mut ingesting_doc.doc);

    Ok(doc)
}

fn add_facet(
    handle: &IndexHandle,
    ingesting_doc: &mut IngestingDocument,
    name: &str,
    value: &[&str],
) -> JavaResult<()> {
    if !name.is_empty() && !value.is_empty() {
        ingesting_doc.doc.add_facet(
            handle.schema.get_field(&facet_field_name(name))?,
            Facet::from_path(value),
        );
    }

    Ok(())
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_removePartKeys(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    keys: JIntArray,
) {
    jni_exec(&mut env, |env| {
        let handle = IndexHandle::get_ref_from_handle(handle);
        let mut terms = vec![];

        let field = handle.schema.get_field(field_constants::PART_ID)?;

        let len = env.get_array_length(&keys)?;
        let mut part_ids = vec![0i32; len as usize];

        env.get_int_array_region(&keys, 0, &mut part_ids)?;

        for part_id in part_ids {
            terms.push(Term::from_field_i64(field, part_id as i64));
        }

        let query = Box::new(TermSetQuery::new(terms));

        let writer = handle.writer.read()?;
        writer.delete_query(query)?;

        handle.changes_pending.store(true, Ordering::SeqCst);

        Ok(())
    })
}

#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_removePartitionsEndedBefore(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
    ended_before: jlong,
    return_deleted_count: jboolean,
) -> jint {
    jni_exec(&mut env, |_| {
        let handle = IndexHandle::get_ref_from_handle(handle);

        let query = RangeQuery::new_i64_bounds(
            field_constants::END_TIME.to_string(),
            Bound::Included(0),
            // To match existing Lucene index behavior, make this inclusive even though it's named
            // "ended before" in the API
            Bound::Included(ended_before),
        );

        let java_ret = if return_deleted_count == JNI_TRUE {
            let searcher = handle.reader.searcher();

            let collector = Count;

            searcher.search(&query, &collector)?
        } else {
            0
        };

        let writer = handle.writer.read()?;
        writer.delete_query(Box::new(query))?;

        handle.changes_pending.store(true, Ordering::SeqCst);

        Ok(java_ret as i32)
    })
}
