//! SIMD-friendly aggregation functions for double vectors.
//!
//! These JNI entry points operate on raw native memory pointers passed from the JVM.
//! The `data_addr` parameter points to the start of the double data (i.e., vector + OffsetData),
//! and `start`/`end` are element indices (inclusive).
//!
//! Uses 4-way accumulator unrolling to enable LLVM auto-vectorization (AVX2 / NEON).

use jni::{
    objects::JClass,
    sys::{jboolean, jdouble, jint, jlong, JNI_TRUE},
    JNIEnv,
};

use crate::{errors::JavaResult, exec::jni_exec};

/// Inner sum implementation with 4-way accumulator unrolling for auto-vectorization.
///
/// When `ignore_nan` is true, NaN values are skipped and the result is NaN only if
/// every element was NaN (matching the Scala semantics where sum starts as Double.NaN).
///
/// When `ignore_nan` is false, all values are summed directly (NaN propagates).
#[inline(always)]
fn sum_double_inner(data_addr: usize, start: usize, end: usize, ignore_nan: bool) -> f64 {
    let count = end - start + 1; // inclusive range
    let base_ptr = data_addr as *const f64;

    if ignore_nan {
        // 4-way accumulator for auto-vectorization
        let mut acc0: f64 = 0.0;
        let mut acc1: f64 = 0.0;
        let mut acc2: f64 = 0.0;
        let mut acc3: f64 = 0.0;
        let mut found_non_nan = false;

        // Process 4 elements at a time
        let chunks = count / 4;
        let remainder = count % 4;

        let mut i = start;
        for _ in 0..chunks {
            // SAFETY: caller guarantees data_addr points to valid memory for [start..=end]
            let v0 = unsafe { *base_ptr.add(i) };
            let v1 = unsafe { *base_ptr.add(i + 1) };
            let v2 = unsafe { *base_ptr.add(i + 2) };
            let v3 = unsafe { *base_ptr.add(i + 3) };

            // IEEE 754: NaN != NaN, so v == v is true only for non-NaN
            if v0 == v0 {
                acc0 += v0;
                found_non_nan = true;
            }
            if v1 == v1 {
                acc1 += v1;
                found_non_nan = true;
            }
            if v2 == v2 {
                acc2 += v2;
                found_non_nan = true;
            }
            if v3 == v3 {
                acc3 += v3;
                found_non_nan = true;
            }
            i += 4;
        }

        // Handle remaining elements
        for _ in 0..remainder {
            let v = unsafe { *base_ptr.add(i) };
            if v == v {
                acc0 += v;
                found_non_nan = true;
            }
            i += 1;
        }

        if found_non_nan {
            acc0 + acc1 + acc2 + acc3
        } else {
            f64::NAN
        }
    } else {
        // No NaN checking — 4-way accumulator for auto-vectorization
        let mut acc0: f64 = 0.0;
        let mut acc1: f64 = 0.0;
        let mut acc2: f64 = 0.0;
        let mut acc3: f64 = 0.0;

        let chunks = count / 4;
        let remainder = count % 4;

        let mut i = start;
        for _ in 0..chunks {
            let v0 = unsafe { *base_ptr.add(i) };
            let v1 = unsafe { *base_ptr.add(i + 1) };
            let v2 = unsafe { *base_ptr.add(i + 2) };
            let v3 = unsafe { *base_ptr.add(i + 3) };

            acc0 += v0;
            acc1 += v1;
            acc2 += v2;
            acc3 += v3;

            i += 4;
        }

        for _ in 0..remainder {
            let v = unsafe { *base_ptr.add(i) };
            acc0 += v;
            i += 1;
        }

        acc0 + acc1 + acc2 + acc3
    }
}

/// Inner count implementation — counts non-NaN values.
#[inline(always)]
fn count_double_inner(data_addr: usize, start: usize, end: usize) -> i32 {
    let count = end - start + 1;
    let base_ptr = data_addr as *const f64;
    let mut total: i32 = 0;

    for idx in start..start + count {
        let v = unsafe { *base_ptr.add(idx) };
        // IEEE 754: NaN != NaN
        if v == v {
            total += 1;
        }
    }

    total
}

// ── JNI entry points ──────────────────────────────────────────────────

/// JNI: Sum doubles from native memory with optional NaN skipping.
///
/// Scala companion: `SimdNativeMethods.simdSumDouble`
///
/// # Arguments
/// * `data_addr` — pointer to first double element (vector + OffsetData)
/// * `start` — first element index (inclusive)
/// * `end` — last element index (inclusive)
/// * `ignore_nan` — JNI boolean; if true, skip NaN values
#[no_mangle]
pub extern "system" fn Java_filodb_memory_format_vectors_SimdNativeMethods_00024_simdSumDouble(
    mut env: JNIEnv,
    _class: JClass,
    data_addr: jlong,
    start: jint,
    end: jint,
    ignore_nan: jboolean,
) -> jdouble {
    jni_exec(&mut env, |_| -> JavaResult<f64> {
        let result = sum_double_inner(
            data_addr as usize,
            start as usize,
            end as usize,
            ignore_nan == JNI_TRUE,
        );
        Ok(result)
    })
}

/// JNI: Count non-NaN doubles from native memory.
///
/// Scala companion: `SimdNativeMethods.simdCountDouble`
///
/// # Arguments
/// * `data_addr` — pointer to first double element (vector + OffsetData)
/// * `start` — first element index (inclusive)
/// * `end` — last element index (inclusive)
#[no_mangle]
pub extern "system" fn Java_filodb_memory_format_vectors_SimdNativeMethods_00024_simdCountDouble(
    mut env: JNIEnv,
    _class: JClass,
    data_addr: jlong,
    start: jint,
    end: jint,
) -> jint {
    jni_exec(&mut env, |_| -> JavaResult<i32> {
        let result = count_double_inner(data_addr as usize, start as usize, end as usize);
        Ok(result)
    })
}
