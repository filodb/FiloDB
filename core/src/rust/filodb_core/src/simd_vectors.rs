//! SIMD-friendly aggregation functions for double vectors.
//!
//! These JNI entry points operate on raw native memory pointers passed from the JVM.
//! The `data_addr` parameter points to the start of the double data (i.e., vector + OffsetData),
//! and `start`/`end` are element indices (inclusive).
//!
//! Uses branchless bitmask NaN-zeroing and 8-way accumulator unrolling so that
//! LLVM can auto-vectorize to NEON (aarch64) or SSE2/AVX2 (x86_64) without
//! platform-specific intrinsics.

// `v == v` is the IEEE 754 NaN detection idiom — false only for NaN.
#![allow(clippy::eq_op)]

use jni::{
    objects::JClass,
    sys::{jboolean, jdouble, jint, jlong, JNI_TRUE},
    JNIEnv,
};

use crate::{errors::JavaResult, exec::jni_exec};

/// Branchless NaN → 0.0 replacement.
///
/// ```text
/// not_nan = (v == v) as u64        → 0 for NaN, 1 for valid    (cset / setcc, no branch)
/// mask    = 0u64.wrapping_sub(…)   → 0x0 for NaN, 0xFFFF…F for valid
/// result  = v.to_bits() & mask     → zeroes all bits if NaN
/// ```
///
/// This is branchless and maps directly to SIMD:
///   NEON:  fcmeq.2d + and.16b
///   SSE2:  cmpeq_pd + andpd
#[inline(always)]
fn zero_nan(v: f64) -> f64 {
    let not_nan = (v == v) as u64;
    let mask = 0u64.wrapping_sub(not_nan);
    f64::from_bits(v.to_bits() & mask)
}

/// Sum doubles with 8-way accumulator unrolling for auto-vectorization.
///
/// When `ignore_nan` is true, NaN values are replaced with 0.0 via branchless bitmask
/// so the add is unconditional — enabling LLVM to emit SIMD instructions.
/// Returns NaN only if every element was NaN (matching Scala semantics).
///
/// When `ignore_nan` is false, all values are summed directly (NaN propagates).
#[inline(always)]
fn sum_double_inner(data_addr: usize, start: usize, end: usize, ignore_nan: bool) -> f64 {
    let count = end - start + 1; // inclusive range
    let base_ptr = data_addr as *const f64;

    // 8 independent accumulators → LLVM maps to 4 vector registers (2 doubles each).
    let mut acc0: f64 = 0.0;
    let mut acc1: f64 = 0.0;
    let mut acc2: f64 = 0.0;
    let mut acc3: f64 = 0.0;
    let mut acc4: f64 = 0.0;
    let mut acc5: f64 = 0.0;
    let mut acc6: f64 = 0.0;
    let mut acc7: f64 = 0.0;

    let chunks = count / 8;
    let remainder = count % 8;
    let mut i = start;

    if ignore_nan {
        let mut non_nan_count: u32 = 0;

        for _ in 0..chunks {
            // SAFETY: caller guarantees data_addr points to valid memory for [start..=end]
            let v0 = unsafe { *base_ptr.add(i) };
            let v1 = unsafe { *base_ptr.add(i + 1) };
            let v2 = unsafe { *base_ptr.add(i + 2) };
            let v3 = unsafe { *base_ptr.add(i + 3) };
            let v4 = unsafe { *base_ptr.add(i + 4) };
            let v5 = unsafe { *base_ptr.add(i + 5) };
            let v6 = unsafe { *base_ptr.add(i + 6) };
            let v7 = unsafe { *base_ptr.add(i + 7) };

            // Branchless: NaN → 0.0, valid → unchanged
            acc0 += zero_nan(v0);
            acc1 += zero_nan(v1);
            acc2 += zero_nan(v2);
            acc3 += zero_nan(v3);
            acc4 += zero_nan(v4);
            acc5 += zero_nan(v5);
            acc6 += zero_nan(v6);
            acc7 += zero_nan(v7);

            // Count non-NaN values
            non_nan_count += (v0 == v0) as u32 + (v1 == v1) as u32
                + (v2 == v2) as u32 + (v3 == v3) as u32
                + (v4 == v4) as u32 + (v5 == v5) as u32
                + (v6 == v6) as u32 + (v7 == v7) as u32;

            i += 8;
        }

        for _ in 0..remainder {
            let v = unsafe { *base_ptr.add(i) };
            acc0 += zero_nan(v);
            non_nan_count += (v == v) as u32;
            i += 1;
        }

        if non_nan_count > 0 {
            acc0 + acc1 + acc2 + acc3 + acc4 + acc5 + acc6 + acc7
        } else {
            f64::NAN
        }
    } else {
        for _ in 0..chunks {
            let v0 = unsafe { *base_ptr.add(i) };
            let v1 = unsafe { *base_ptr.add(i + 1) };
            let v2 = unsafe { *base_ptr.add(i + 2) };
            let v3 = unsafe { *base_ptr.add(i + 3) };
            let v4 = unsafe { *base_ptr.add(i + 4) };
            let v5 = unsafe { *base_ptr.add(i + 5) };
            let v6 = unsafe { *base_ptr.add(i + 6) };
            let v7 = unsafe { *base_ptr.add(i + 7) };

            acc0 += v0;
            acc1 += v1;
            acc2 += v2;
            acc3 += v3;
            acc4 += v4;
            acc5 += v5;
            acc6 += v6;
            acc7 += v7;

            i += 8;
        }

        for _ in 0..remainder {
            let v = unsafe { *base_ptr.add(i) };
            acc0 += v;
            i += 1;
        }

        acc0 + acc1 + acc2 + acc3 + acc4 + acc5 + acc6 + acc7
    }
}

/// Count non-NaN doubles.
///
/// Simple loop — LLVM auto-vectorizes this aggressively (16 doubles/iter on aarch64
/// using fcmeq.2d → uzp1.4s → sub.4s).
#[inline(always)]
fn count_double_inner(data_addr: usize, start: usize, end: usize) -> i32 {
    let count = end - start + 1;
    let base_ptr = data_addr as *const f64;
    let mut total: i32 = 0;
    for idx in start..start + count {
        let v = unsafe { *base_ptr.add(idx) };
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
