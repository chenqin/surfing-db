// SIMD utilities for high-performance Thrift and Parquet operations
// Supports AVX2, SSE4.2, and ARM NEON

#ifndef SURFING_SIMD_UTILS_H
#define SURFING_SIMD_UTILS_H

#include <cstdint>
#include <cstring>

// Detect SIMD support
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
  #define SURFING_X86
  #ifdef __AVX2__
    #define SURFING_AVX2
    #include <immintrin.h>
  #elif __SSE4_2__
    #define SURFING_SSE42
    #include <nmmintrin.h>
  #elif __SSSE3__
    #define SURFING_SSSE3
    #include <tmmintrin.h>
  #endif
#elif defined(__ARM_NEON) || defined(__aarch64__)
  #define SURFING_NEON
  #include <arm_neon.h>
#endif

namespace surfing {
namespace simd {

// ============================================================================
// Byte Swapping (Endianness Conversion)
// ============================================================================

// Scalar fallback
inline uint32_t bswap32_scalar(uint32_t x) {
  return __builtin_bswap32(x);
}

inline uint64_t bswap64_scalar(uint64_t x) {
  return __builtin_bswap64(x);
}

#ifdef SURFING_SSSE3
// SIMD byte swap for 32-bit integers (8 values at once using SSSE3)
inline void bswap32_simd(const uint32_t* src, uint32_t* dst, size_t count) {
  const __m128i shuffle_mask = _mm_set_epi8(
    12, 13, 14, 15,  // Swap bytes of fourth 32-bit int
    8, 9, 10, 11,    // Swap bytes of third 32-bit int
    4, 5, 6, 7,      // Swap bytes of second 32-bit int
    0, 1, 2, 3       // Swap bytes of first 32-bit int
  );

  size_t i = 0;
  // Process 4 uint32_t (16 bytes) at a time
  for (; i + 4 <= count; i += 4) {
    __m128i data = _mm_loadu_si128(reinterpret_cast<const __m128i*>(src + i));
    __m128i swapped = _mm_shuffle_epi8(data, shuffle_mask);
    _mm_storeu_si128(reinterpret_cast<__m128i*>(dst + i), swapped);
  }

  // Handle remaining elements
  for (; i < count; ++i) {
    dst[i] = bswap32_scalar(src[i]);
  }
}
#endif

#ifdef SURFING_AVX2
// AVX2 byte swap for 32-bit integers (16 values at once)
inline void bswap32_avx2(const uint32_t* src, uint32_t* dst, size_t count) {
  const __m256i shuffle_mask = _mm256_set_epi8(
    12, 13, 14, 15, 8, 9, 10, 11, 4, 5, 6, 7, 0, 1, 2, 3,
    12, 13, 14, 15, 8, 9, 10, 11, 4, 5, 6, 7, 0, 1, 2, 3
  );

  size_t i = 0;
  // Process 8 uint32_t (32 bytes) at a time
  for (; i + 8 <= count; i += 8) {
    __m256i data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + i));
    __m256i swapped = _mm256_shuffle_epi8(data, shuffle_mask);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + i), swapped);
  }

  // Handle remaining with SSE or scalar
  for (; i < count; ++i) {
    dst[i] = bswap32_scalar(src[i]);
  }
}
#endif

#ifdef SURFING_NEON
// NEON byte swap for 32-bit integers (4 values at once)
inline void bswap32_neon(const uint32_t* src, uint32_t* dst, size_t count) {
  size_t i = 0;
  // Process 4 uint32_t at a time
  for (; i + 4 <= count; i += 4) {
    uint32x4_t data = vld1q_u32(src + i);
    uint32x4_t swapped = vrev32q_u8(vreinterpretq_u8_u32(data));
    vst1q_u32(dst + i, vreinterpretq_u32_u8(swapped));
  }

  // Handle remaining
  for (; i < count; ++i) {
    dst[i] = __builtin_bswap32(src[i]);
  }
}
#endif

// Generic dispatcher
inline void bswap32_bulk(const uint32_t* src, uint32_t* dst, size_t count) {
#ifdef SURFING_AVX2
  bswap32_avx2(src, dst, count);
#elif defined(SURFING_SSSE3)
  bswap32_simd(src, dst, count);
#elif defined(SURFING_NEON)
  bswap32_neon(src, dst, count);
#else
  for (size_t i = 0; i < count; ++i) {
    dst[i] = bswap32_scalar(src[i]);
  }
#endif
}

// ============================================================================
// Memory Copy with Prefetching
// ============================================================================

#ifdef SURFING_SSE42
inline void memcpy_prefetch(void* dst, const void* src, size_t n) {
  const size_t prefetch_distance = 256;
  const uint8_t* s = static_cast<const uint8_t*>(src);
  uint8_t* d = static_cast<uint8_t*>(dst);

  size_t i = 0;
  // Prefetch ahead
  for (; i + prefetch_distance <= n; i += 64) {
    _mm_prefetch(reinterpret_cast<const char*>(s + i + prefetch_distance), _MM_HINT_T0);
    __m128i data0 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s + i));
    __m128i data1 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s + i + 16));
    __m128i data2 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s + i + 32));
    __m128i data3 = _mm_loadu_si128(reinterpret_cast<const __m128i*>(s + i + 48));

    _mm_storeu_si128(reinterpret_cast<__m128i*>(d + i), data0);
    _mm_storeu_si128(reinterpret_cast<__m128i*>(d + i + 16), data1);
    _mm_storeu_si128(reinterpret_cast<__m128i*>(d + i + 32), data2);
    _mm_storeu_si128(reinterpret_cast<__m128i*>(d + i + 48), data3);
  }

  // Handle remaining
  if (i < n) {
    std::memcpy(d + i, s + i, n - i);
  }
}
#else
inline void memcpy_prefetch(void* dst, const void* src, size_t n) {
  std::memcpy(dst, src, n);
}
#endif

// ============================================================================
// Fast String Length (for Thrift string parsing)
// ============================================================================

#ifdef SURFING_SSE42
inline size_t strlen_simd(const char* str) {
  const __m128i zero = _mm_setzero_si128();
  const char* ptr = str;

  // Align to 16-byte boundary
  size_t misalignment = reinterpret_cast<uintptr_t>(ptr) & 15;
  if (misalignment) {
    size_t align_bytes = 16 - misalignment;
    for (size_t i = 0; i < align_bytes; ++i) {
      if (ptr[i] == '\0') return i;
    }
    ptr += align_bytes;
  }

  // Process 16 bytes at a time
  while (true) {
    __m128i data = _mm_load_si128(reinterpret_cast<const __m128i*>(ptr));
    __m128i cmp = _mm_cmpeq_epi8(data, zero);
    int mask = _mm_movemask_epi8(cmp);

    if (mask != 0) {
      int pos = __builtin_ctz(mask);
      return (ptr - str) + pos;
    }

    ptr += 16;
  }
}
#else
inline size_t strlen_simd(const char* str) {
  return std::strlen(str);
}
#endif

// ============================================================================
// Vectorized Validity Bitmap Construction
// ============================================================================

#ifdef SURFING_AVX2
// Pack 32 validity bytes into 4-byte bitmap using AVX2
inline uint32_t pack_validity_avx2(const uint8_t* valid, size_t count) {
  if (count != 32) {
    // Fallback for non-32 counts
    uint32_t bitmap = 0;
    for (size_t i = 0; i < count && i < 32; ++i) {
      if (valid[i]) bitmap |= (1u << i);
    }
    return bitmap;
  }

  __m256i data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(valid));
  __m256i zero = _mm256_setzero_si256();
  __m256i cmp = _mm256_cmpgt_epi8(data, zero);

  return static_cast<uint32_t>(_mm256_movemask_epi8(cmp));
}
#endif

#ifdef SURFING_SSE42
// Pack 16 validity bytes into 2-byte bitmap using SSE
inline uint16_t pack_validity_sse(const uint8_t* valid, size_t count) {
  if (count != 16) {
    uint16_t bitmap = 0;
    for (size_t i = 0; i < count && i < 16; ++i) {
      if (valid[i]) bitmap |= (1u << i);
    }
    return bitmap;
  }

  __m128i data = _mm_loadu_si128(reinterpret_cast<const __m128i*>(valid));
  __m128i zero = _mm_setzero_si128();
  __m128i cmp = _mm_cmpgt_epi8(data, zero);

  return static_cast<uint16_t>(_mm_movemask_epi8(cmp));
}
#endif

// ============================================================================
// Horizontal Sum (for row counting)
// ============================================================================

#ifdef SURFING_AVX2
inline int32_t horizontal_sum_avx2(__m256i v) {
  __m128i low = _mm256_castsi256_si128(v);
  __m128i high = _mm256_extracti128_si256(v, 1);
  __m128i sum128 = _mm_add_epi32(low, high);

  __m128i sum64 = _mm_hadd_epi32(sum128, sum128);
  __m128i sum32 = _mm_hadd_epi32(sum64, sum64);

  return _mm_cvtsi128_si32(sum32);
}
#endif

#ifdef SURFING_SSE42
inline int32_t horizontal_sum_sse(__m128i v) {
  __m128i sum64 = _mm_hadd_epi32(v, v);
  __m128i sum32 = _mm_hadd_epi32(sum64, sum64);
  return _mm_cvtsi128_si32(sum32);
}
#endif

// ============================================================================
// Bulk Integer Parsing (Thrift LIST<I64>)
// ============================================================================

#ifdef SURFING_AVX2
// Parse and byte-swap 4 int64_t values at once
inline void parse_i64_bulk_avx2(const uint8_t* src, int64_t* dst, size_t count) {
  const __m256i shuffle_mask = _mm256_set_epi8(
    8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7,
    8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3, 4, 5, 6, 7
  );

  size_t i = 0;
  for (; i + 4 <= count; i += 4) {
    __m256i data = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + i * 8));
    __m256i swapped = _mm256_shuffle_epi8(data, shuffle_mask);
    _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + i), swapped);
  }

  // Handle remaining
  for (; i < count; ++i) {
    uint64_t be = *reinterpret_cast<const uint64_t*>(src + i * 8);
    dst[i] = static_cast<int64_t>(__builtin_bswap64(be));
  }
}
#endif

// ============================================================================
// Feature Detection
// ============================================================================

struct SIMDFeatures {
  bool has_sse42 = false;
  bool has_avx2 = false;
  bool has_neon = false;

  static SIMDFeatures detect() {
    SIMDFeatures features;
#ifdef SURFING_SSE42
    features.has_sse42 = true;
#endif
#ifdef SURFING_AVX2
    features.has_avx2 = true;
#endif
#ifdef SURFING_NEON
    features.has_neon = true;
#endif
    return features;
  }

  const char* description() const {
#ifdef SURFING_AVX2
    return "AVX2";
#elif defined(SURFING_SSE42)
    return "SSE4.2";
#elif defined(SURFING_SSSE3)
    return "SSSE3";
#elif defined(SURFING_NEON)
    return "NEON";
#else
    return "Scalar (no SIMD)";
#endif
  }
};

} // namespace simd
} // namespace surfing

#endif // SURFING_SIMD_UTILS_H
