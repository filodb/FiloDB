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

// ── HistogramVector batch sum ────────────────────────────────────────
//
// Accelerates RowHistogramReader.sum(start, end) for SUBTYPE_H_SIMPLE (delta histograms).
// Walks sections, unpacks NibblePack delta-encoded blobs, and accumulates bucket sums
// — all in one JNI call instead of per-histogram JVM round-trips.
//
// HistogramVector memory layout (off-heap):
//   +0   i32  numBytes (BinaryVector length header)
//   +4   u16  WireFormat
//   +6   u16  numHistograms
//   +8   u8   formatCode
//   +9   u16  bucketDefNumBytes
//   +11  [u8] bucket definition
//   +(11+bucketDefNumBytes)  Sections...
//
// Section layout:
//   +0  u16  sectionNumBytes (data after this 4-byte header)
//   +2  u8   numElements (max 255)
//   +3  u8   sectionType (0=Normal, 1=Drop)
//   +4  Blobs: [u16 blobLen][NibblePacked delta values]...

const HIST_OFFSET_BUCKET_DEF_SIZE: usize = 9;
const HIST_OFFSET_BUCKET_DEF: usize = 11;

/// Read a little-endian u16 from a raw pointer at byte offset.
#[inline(always)]
unsafe fn read_u16_at(base: *const u8, offset: usize) -> u16 {
    let ptr = base.add(offset) as *const u16;
    u16::from_le(ptr.read_unaligned())
}

/// Read a u8 from a raw pointer at byte offset.
#[inline(always)]
unsafe fn read_u8_at(base: *const u8, offset: usize) -> u8 {
    *base.add(offset)
}

/// Read a little-endian i64, handling end-of-buffer safely (< 8 bytes remaining).
#[inline(always)]
unsafe fn read_i64_safe(ptr: *const u8, remaining: usize) -> i64 {
    if remaining >= 8 {
        i64::from_le((ptr as *const i64).read_unaligned())
    } else {
        let mut word: i64 = 0;
        for i in 0..remaining {
            word |= (*ptr.add(i) as i64 & 0xff) << (8 * i);
        }
        word
    }
}

/// Unpack 8 NibblePacked values. Returns bytes consumed, or 0 on error.
/// Direct port of Scala NibblePack.unpack8().
#[inline(always)]
unsafe fn nibble_unpack8(input: *const u8, input_len: usize, output: &mut [i64; 8]) -> usize {
    if input_len == 0 { return 0; }

    let nonzero_mask = *input as u32;
    if nonzero_mask == 0 {
        *output = [0i64; 8];
        return 1;
    }
    if input_len < 2 { return 0; }

    let nibble_word = *input.add(1) as u32 & 0xff;
    let num_bits = ((nibble_word >> 4) + 1) * 4;
    let trailing_zeroes = (nibble_word & 0x0f) * 4;
    let total_bytes = 2 + (num_bits * (nonzero_mask.count_ones()) + 7) / 8;

    if (total_bytes as usize) > input_len { return 0; }

    let mask: i64 = if num_bits >= 64 { -1i64 } else { (1i64 << num_bits) - 1 };
    let mut buf_index: usize = 2;
    let mut bit_cursor: u32 = 0;

    let mut in_word = read_i64_safe(input.add(buf_index), input_len - buf_index);
    buf_index += 8;

    for bit in 0..8u32 {
        if (nonzero_mask & (1 << bit)) != 0 {
            let remaining = 64 - bit_cursor;
            let shifted_in = ((in_word as u64) >> bit_cursor) as i64;
            let mut out_word = shifted_in & mask;

            if remaining <= num_bits && buf_index < total_bytes as usize {
                if buf_index < input_len {
                    in_word = read_i64_safe(input.add(buf_index), input_len - buf_index);
                    buf_index += 8;
                    if remaining < num_bits {
                        out_word |= (in_word << remaining as i64) & mask;
                    }
                } else {
                    return 0;
                }
            }
            output[bit as usize] = out_word << trailing_zeroes;
            bit_cursor = (bit_cursor + num_bits) % 64;
        } else {
            output[bit as usize] = 0;
        }
    }
    total_bytes as usize
}

/// Unpack a full NibblePack delta-encoded blob into cumulative Long[] bucket values.
/// Port of Scala DeltaSink: prefix-sum of deltas to reconstruct cumulative values.
#[inline(always)]
unsafe fn nibble_unpack_delta(
    blob_data: *const u8, blob_len: usize, values: &mut [i64], num_buckets: usize,
) -> bool {
    let mut offset: usize = 0;
    let mut current: i64 = 0;
    let mut bucket_idx: usize = 0;
    let mut raw = [0i64; 8];

    while bucket_idx < num_buckets && offset < blob_len {
        let consumed = nibble_unpack8(blob_data.add(offset), blob_len - offset, &mut raw);
        if consumed == 0 { return false; }
        offset += consumed;

        let elems = std::cmp::min(8, num_buckets - bucket_idx);
        for n in 0..elems {
            current += raw[n];
            values[bucket_idx + n] = current;
        }
        bucket_idx += 8;
    }
    true
}

/// Batch sum of histogram bucket values across rows [start_row, end_row] inclusive.
/// Returns number of histograms summed, or -1 on error.
fn histogram_batch_sum_inner(
    vector_addr: usize, start_row: usize, end_row: usize,
    out_addr: usize, num_buckets: usize,
) -> i32 {
    let base = vector_addr as *const u8;
    let out = out_addr as *mut f64;

    // Zero the output accumulator
    for i in 0..num_buckets {
        unsafe { *out.add(i) = 0.0; }
    }

    // Parse header
    let bucket_def_num_bytes = unsafe { read_u16_at(base, HIST_OFFSET_BUCKET_DEF_SIZE) } as usize;
    let first_section_addr = vector_addr + HIST_OFFSET_BUCKET_DEF + bucket_def_num_bytes;
    let vector_total_bytes = unsafe { (base as *const i32).read_unaligned().to_le() } as usize + 4;
    let vector_end = vector_addr + vector_total_bytes;

    // Temp buffer for one histogram's bucket values
    let mut values = vec![0i64; num_buckets];
    let mut summed: i32 = 0;
    let mut global_elem: usize = 0;
    let mut section_addr = first_section_addr;

    // Walk sections
    while global_elem <= end_row && section_addr < vector_end {
        let sect_base = section_addr as *const u8;
        let sect_num_bytes = unsafe { read_u16_at(sect_base, 0) } as usize;
        let sect_num_elems = unsafe { read_u8_at(sect_base, 2) } as usize;

        if sect_num_elems == 0 { break; }

        let mut blob_ptr = section_addr + 4; // skip 4-byte section header
        let sect_data_end = section_addr + 4 + sect_num_bytes;

        for _local_i in 0..sect_num_elems {
            if blob_ptr >= sect_data_end || blob_ptr >= vector_end { break; }

            let blob_base = blob_ptr as *const u8;
            let blob_len = unsafe { read_u16_at(blob_base, 0) } as usize;

            if global_elem >= start_row && global_elem <= end_row {
                for v in values.iter_mut() { *v = 0; }

                let ok = unsafe {
                    nibble_unpack_delta((blob_ptr + 2) as *const u8, blob_len, &mut values, num_buckets)
                };
                if ok {
                    for b in 0..num_buckets {
                        unsafe { *out.add(b) += values[b] as f64; }
                    }
                    summed += 1;
                }
            }

            blob_ptr += 2 + blob_len;
            global_elem += 1;
            if global_elem > end_row { break; }
        }

        section_addr = sect_data_end;
    }
    summed
}

/// JNI: Batch sum of histogram bucket values for SUBTYPE_H_SIMPLE vectors.
///
/// Scala companion: `SimdNativeMethods.histogramBatchSum`
#[no_mangle]
pub extern "system" fn Java_filodb_memory_format_vectors_SimdNativeMethods_00024_histogramBatchSum(
    mut env: JNIEnv,
    _class: JClass,
    vector_addr: jlong,
    start_row: jint,
    end_row: jint,
    out_addr: jlong,
    num_buckets: jint,
) -> jint {
    jni_exec(&mut env, |_| -> JavaResult<i32> {
        let result = histogram_batch_sum_inner(
            vector_addr as usize, start_row as usize, end_row as usize,
            out_addr as usize, num_buckets as usize,
        );
        Ok(result)
    })
}
