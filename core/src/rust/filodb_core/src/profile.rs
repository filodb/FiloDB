//! Helpers for profiling / testing

#[cfg(feature = "dhat-heap")]
use std::sync::Mutex;

use jni::{
    objects::JClass,
    sys::{jlong, jstring},
    JNIEnv,
};

use crate::{exec::jni_exec, state::IndexHandle};

/// Get cache stats info
#[no_mangle]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_dumpCacheStats(
    mut env: JNIEnv,
    _class: JClass,
    handle: jlong,
) -> jstring {
    jni_exec(&mut env, |env| {
        let index = IndexHandle::get_ref_from_handle(handle);

        let (column_hits, column_misses) = index.column_cache.stats();
        let (query_hits, query_misses) = index.query_cache_stats();

        let output = format!(
            "Column cache: {} hits {} misses\nQuery cache: {} hits {} misses",
            column_hits, column_misses, query_hits, query_misses
        );

        let java_str = env.new_string(output)?;

        Ok(java_str.into_raw())
    })
}

/// Start memory profiling
#[no_mangle]
#[allow(unused_mut, unused_variables)]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_startMemoryProfiling(
    mut env: JNIEnv,
    _class: JClass,
) {
    #[cfg(feature = "dhat-heap")]
    jni_exec(&mut env, |_| {
        PROFILER.lock()?.replace(dhat::Profiler::new_heap());

        Ok(())
    });
}

/// Stop memory profiling
#[no_mangle]
#[allow(unused_mut, unused_variables)]
pub extern "system" fn Java_filodb_core_memstore_TantivyNativeMethods_00024_stopMemoryProfiling(
    mut env: JNIEnv,
    _class: JClass,
) {
    #[cfg(feature = "dhat-heap")]
    jni_exec(&mut env, |_| {
        PROFILER.lock()?.take();

        Ok(())
    });
}

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

#[cfg(feature = "dhat-heap")]
static PROFILER: Mutex<Option<dhat::Profiler>> = Mutex::new(None);
