// IEEE 754 NaN detection idiom: `v == v` is false only for NaN.
// Clippy flags this as "equal expressions as operands" but it's intentional.
#![allow(clippy::eq_op)]

//! SIMD aggregation functions for double vectors.
//!
//! These JNI entry points operate on raw native memory pointers passed from the JVM.
//! The `data_addr` parameter points to the start of the double data (i.e., vector + OffsetData),
//! and `start`/`end` are element indices (inclusive).
//!
//! Uses explicit SIMD intrinsics: NEON on aarch64, SSE2 on x86_64.
//! Both process 8 doubles per iteration with 4 vector accumulators.
//! Falls back to scalar 4-way unrolled loops on other architectures.

use jni::{
    objects::JClass,
    sys::{jboolean, jdouble, jint, jlong, JNI_TRUE},
    JNIEnv,
};

use crate::{errors::JavaResult, exec::jni_exec};

// ── aarch64 NEON implementation ─────────────────────────────────────────

#[cfg(target_arch = "aarch64")]
use std::arch::aarch64::*;

/// Sum doubles with explicit NEON intrinsics.
///
/// Uses 4 vector accumulators (each holding 2 doubles) to process 8 doubles per iteration,
/// hiding `fadd` latency on Apple Silicon (2-cycle latency, 1-cycle throughput).
///
/// When `ignore_nan` is true, NaN values are zeroed via `vceqq_f64` + `vandq` bitmask
/// so the add is unconditional and fully vectorized (no scalar fallback).
/// Returns NaN only if every element was NaN (matching Scala semantics).
#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn sum_double_inner(data_addr: usize, start: usize, end: usize, ignore_nan: bool) -> f64 {
    let count = end - start + 1;
    let base_ptr = (data_addr as *const f64).wrapping_add(start);

    unsafe {
        let mut acc0 = vdupq_n_f64(0.0);
        let mut acc1 = vdupq_n_f64(0.0);
        let mut acc2 = vdupq_n_f64(0.0);
        let mut acc3 = vdupq_n_f64(0.0);

        let chunks = count / 8;
        let remainder = count % 8;
        let mut ptr = base_ptr;

        if ignore_nan {
            let mut non_nan_acc0 = vdupq_n_u64(0);
            let mut non_nan_acc1 = vdupq_n_u64(0);

            for _ in 0..chunks {
                let v0 = vld1q_f64(ptr);
                let v1 = vld1q_f64(ptr.add(2));
                let v2 = vld1q_f64(ptr.add(4));
                let v3 = vld1q_f64(ptr.add(6));

                let m0 = vceqq_f64(v0, v0);
                let m1 = vceqq_f64(v1, v1);
                let m2 = vceqq_f64(v2, v2);
                let m3 = vceqq_f64(v3, v3);

                let s0: float64x2_t = vreinterpretq_f64_u64(vandq_u64(vreinterpretq_u64_f64(v0), m0));
                let s1: float64x2_t = vreinterpretq_f64_u64(vandq_u64(vreinterpretq_u64_f64(v1), m1));
                let s2: float64x2_t = vreinterpretq_f64_u64(vandq_u64(vreinterpretq_u64_f64(v2), m2));
                let s3: float64x2_t = vreinterpretq_f64_u64(vandq_u64(vreinterpretq_u64_f64(v3), m3));

                acc0 = vaddq_f64(acc0, s0);
                acc1 = vaddq_f64(acc1, s1);
                acc2 = vaddq_f64(acc2, s2);
                acc3 = vaddq_f64(acc3, s3);

                non_nan_acc0 = vsubq_u64(non_nan_acc0, m0);
                non_nan_acc0 = vsubq_u64(non_nan_acc0, m1);
                non_nan_acc1 = vsubq_u64(non_nan_acc1, m2);
                non_nan_acc1 = vsubq_u64(non_nan_acc1, m3);

                ptr = ptr.add(8);
            }

            let mut rem_sum = 0.0f64;
            let mut rem_non_nan = 0u32;
            for j in 0..remainder {
                let v = *ptr.add(j);
                if v == v { rem_sum += v; rem_non_nan += 1; }
            }

            let sum01 = vaddq_f64(acc0, acc1);
            let sum23 = vaddq_f64(acc2, acc3);
            let sum0123 = vaddq_f64(sum01, sum23);
            let total = vgetq_lane_f64(sum0123, 0) + vgetq_lane_f64(sum0123, 1) + rem_sum;

            let nn = vaddq_u64(non_nan_acc0, non_nan_acc1);
            let nn_total = vgetq_lane_u64(nn, 0) + vgetq_lane_u64(nn, 1) + rem_non_nan as u64;

            if nn_total > 0 { total } else { f64::NAN }
        } else {
            for _ in 0..chunks {
                let v0 = vld1q_f64(ptr);
                let v1 = vld1q_f64(ptr.add(2));
                let v2 = vld1q_f64(ptr.add(4));
                let v3 = vld1q_f64(ptr.add(6));

                acc0 = vaddq_f64(acc0, v0);
                acc1 = vaddq_f64(acc1, v1);
                acc2 = vaddq_f64(acc2, v2);
                acc3 = vaddq_f64(acc3, v3);

                ptr = ptr.add(8);
            }

            let mut rem_sum = 0.0f64;
            for j in 0..remainder {
                rem_sum += *ptr.add(j);
            }

            let sum01 = vaddq_f64(acc0, acc1);
            let sum23 = vaddq_f64(acc2, acc3);
            let sum0123 = vaddq_f64(sum01, sum23);
            vgetq_lane_f64(sum0123, 0) + vgetq_lane_f64(sum0123, 1) + rem_sum
        }
    }
}

/// Count non-NaN doubles (aarch64).
///
/// Simple scalar loop — LLVM auto-vectorizes this to 16 doubles/iteration using
/// `fcmeq.2d` → `uzp1.4s` narrowing → `sub.4s`, outperforming hand-written NEON.
#[cfg(target_arch = "aarch64")]
#[inline(always)]
fn count_double_inner(data_addr: usize, start: usize, end: usize) -> i32 {
    let count = end - start + 1;
    let base_ptr = data_addr as *const f64;
    let mut total: i32 = 0;
    for idx in start..start + count {
        let v = unsafe { *base_ptr.add(idx) };
        if v == v { total += 1; }
    }
    total
}

// ── x86_64 SSE2 implementation ──────────────────────────────────────────

#[cfg(target_arch = "x86_64")]
use std::arch::x86_64::*;

/// Sum doubles with SSE2 intrinsics (available on all x86_64 CPUs).
///
/// SSE2 `__m128d` holds 2 doubles, same layout as NEON `float64x2_t`.
/// Uses 4 vector accumulators to process 8 doubles per iteration.
///
/// When `ignore_nan` is true, NaN values are zeroed via `_mm_cmpeq_pd` + `_mm_and_pd`
/// bitmask so the add is unconditional and fully vectorized.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn sum_double_inner(data_addr: usize, start: usize, end: usize, ignore_nan: bool) -> f64 {
    let count = end - start + 1;
    let base_ptr = (data_addr as *const f64).wrapping_add(start);

    unsafe {
        let mut acc0 = _mm_setzero_pd();
        let mut acc1 = _mm_setzero_pd();
        let mut acc2 = _mm_setzero_pd();
        let mut acc3 = _mm_setzero_pd();

        let chunks = count / 8;
        let remainder = count % 8;
        let mut ptr = base_ptr;

        if ignore_nan {
            // Use i64 for counting via mask bits
            let mut non_nan_count: u64 = 0;

            for _ in 0..chunks {
                let v0 = _mm_loadu_pd(ptr);
                let v1 = _mm_loadu_pd(ptr.add(2));
                let v2 = _mm_loadu_pd(ptr.add(4));
                let v3 = _mm_loadu_pd(ptr.add(6));

                // cmpeq_pd: ordered equality — NaN != NaN → 0, non-NaN == non-NaN → all-1s
                let m0 = _mm_cmpeq_pd(v0, v0);
                let m1 = _mm_cmpeq_pd(v1, v1);
                let m2 = _mm_cmpeq_pd(v2, v2);
                let m3 = _mm_cmpeq_pd(v3, v3);

                // Zero out NaN lanes, then add unconditionally
                acc0 = _mm_add_pd(acc0, _mm_and_pd(v0, m0));
                acc1 = _mm_add_pd(acc1, _mm_and_pd(v1, m1));
                acc2 = _mm_add_pd(acc2, _mm_and_pd(v2, m2));
                acc3 = _mm_add_pd(acc3, _mm_and_pd(v3, m3));

                // Count non-NaN via movemask: 2 bits per __m128d, each bit = 1 if non-NaN
                non_nan_count += (_mm_movemask_pd(m0) as u64).count_ones() as u64;
                non_nan_count += (_mm_movemask_pd(m1) as u64).count_ones() as u64;
                non_nan_count += (_mm_movemask_pd(m2) as u64).count_ones() as u64;
                non_nan_count += (_mm_movemask_pd(m3) as u64).count_ones() as u64;

                ptr = ptr.add(8);
            }

            // Scalar remainder
            let mut rem_sum = 0.0f64;
            for j in 0..remainder {
                let v = *ptr.add(j);
                if v == v { rem_sum += v; non_nan_count += 1; }
            }

            // Horizontal reduction: acc0 + acc1 + acc2 + acc3
            let sum01 = _mm_add_pd(acc0, acc1);
            let sum23 = _mm_add_pd(acc2, acc3);
            let sum0123 = _mm_add_pd(sum01, sum23);
            // Extract both lanes: _mm_cvtsd_f64 gets low, shuffle+extract gets high
            let high = _mm_unpackhi_pd(sum0123, sum0123);
            let total = _mm_cvtsd_f64(sum0123) + _mm_cvtsd_f64(high) + rem_sum;

            if non_nan_count > 0 { total } else { f64::NAN }
        } else {
            for _ in 0..chunks {
                let v0 = _mm_loadu_pd(ptr);
                let v1 = _mm_loadu_pd(ptr.add(2));
                let v2 = _mm_loadu_pd(ptr.add(4));
                let v3 = _mm_loadu_pd(ptr.add(6));

                acc0 = _mm_add_pd(acc0, v0);
                acc1 = _mm_add_pd(acc1, v1);
                acc2 = _mm_add_pd(acc2, v2);
                acc3 = _mm_add_pd(acc3, v3);

                ptr = ptr.add(8);
            }

            let mut rem_sum = 0.0f64;
            for j in 0..remainder {
                rem_sum += *ptr.add(j);
            }

            let sum01 = _mm_add_pd(acc0, acc1);
            let sum23 = _mm_add_pd(acc2, acc3);
            let sum0123 = _mm_add_pd(sum01, sum23);
            let high = _mm_unpackhi_pd(sum0123, sum0123);
            _mm_cvtsd_f64(sum0123) + _mm_cvtsd_f64(high) + rem_sum
        }
    }
}

/// Count non-NaN doubles (x86_64).
///
/// Simple scalar loop — LLVM auto-vectorizes this well on x86_64 with SSE2/AVX2.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
fn count_double_inner(data_addr: usize, start: usize, end: usize) -> i32 {
    let count = end - start + 1;
    let base_ptr = data_addr as *const f64;
    let mut total: i32 = 0;
    for idx in start..start + count {
        let v = unsafe { *base_ptr.add(idx) };
        if v == v { total += 1; }
    }
    total
}

// ── Generic scalar fallback ─────────────────────────────────────────────

#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
#[inline(always)]
fn sum_double_inner(data_addr: usize, start: usize, end: usize, ignore_nan: bool) -> f64 {
    let count = end - start + 1;
    let base_ptr = data_addr as *const f64;

    let mut acc0: f64 = 0.0;
    let mut acc1: f64 = 0.0;
    let mut acc2: f64 = 0.0;
    let mut acc3: f64 = 0.0;

    let chunks = count / 4;
    let remainder = count % 4;
    let mut i = start;

    if ignore_nan {
        let mut found_non_nan = false;
        for _ in 0..chunks {
            let v0 = unsafe { *base_ptr.add(i) };
            let v1 = unsafe { *base_ptr.add(i + 1) };
            let v2 = unsafe { *base_ptr.add(i + 2) };
            let v3 = unsafe { *base_ptr.add(i + 3) };
            if v0 == v0 { acc0 += v0; found_non_nan = true; }
            if v1 == v1 { acc1 += v1; found_non_nan = true; }
            if v2 == v2 { acc2 += v2; found_non_nan = true; }
            if v3 == v3 { acc3 += v3; found_non_nan = true; }
            i += 4;
        }
        for _ in 0..remainder {
            let v = unsafe { *base_ptr.add(i) };
            if v == v { acc0 += v; found_non_nan = true; }
            i += 1;
        }
        if found_non_nan { acc0 + acc1 + acc2 + acc3 } else { f64::NAN }
    } else {
        for _ in 0..chunks {
            acc0 += unsafe { *base_ptr.add(i) };
            acc1 += unsafe { *base_ptr.add(i + 1) };
            acc2 += unsafe { *base_ptr.add(i + 2) };
            acc3 += unsafe { *base_ptr.add(i + 3) };
            i += 4;
        }
        for _ in 0..remainder {
            acc0 += unsafe { *base_ptr.add(i) };
            i += 1;
        }
        acc0 + acc1 + acc2 + acc3
    }
}

#[cfg(not(any(target_arch = "aarch64", target_arch = "x86_64")))]
#[inline(always)]
fn count_double_inner(data_addr: usize, start: usize, end: usize) -> i32 {
    let count = end - start + 1;
    let base_ptr = data_addr as *const f64;
    let mut total: i32 = 0;
    for idx in start..start + count {
        let v = unsafe { *base_ptr.add(idx) };
        if v == v { total += 1; }
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
