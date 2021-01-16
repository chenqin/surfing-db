//
// Created by cq on 1/15/21.
//
#include <cstdint>
#include <glog/logging.h>
#include <immintrin.h>
#include <string>
#include <thread>

#ifndef SURFINGDB_UTILS_H
#define SURFINGDB_UTILS_H

#define DOUBLE_SIZE sizeof(double)

namespace surfingdb {
namespace table {


#define width 32
//https://stackoverflow.com/questions/2963898/faster-alternative-to-memcpy
void fastMemcpy(void* pvDest, const void* pvSrc, size_t nBytes) {
  CHECK(nBytes % width == 0);
  CHECK((intptr_t(pvDest) & 31) == 0);
  CHECK((intptr_t(pvSrc) & 31) == 0);
  const __m256i* pSrc = reinterpret_cast<const __m256i*>(pvSrc);
  __m256i* pDest = reinterpret_cast<__m256i*>(pvDest);
  int64_t nVects = nBytes / sizeof(*pSrc);
  for (; nVects > 0; nVects--, pSrc++, pDest++) {
    const __m256i loaded = _mm256_stream_load_si256(pSrc);
    _mm256_stream_si256(pDest, loaded);
  }
  _mm_sfence();
}

/*
 * check if byte size align with avx
 */
void fastcpy(void* pvDest, const void* pvSrc, size_t nBytes) {
  size_t fastCopyBytes = nBytes - nBytes % width;
  if (fastCopyBytes > 0) {
    fastMemcpy(pvDest, pvSrc, fastCopyBytes);
  }
  if (fastCopyBytes != nBytes) {
    memcpy((char*)pvDest + fastCopyBytes, (char*)pvSrc + fastCopyBytes, nBytes % width);
  }
}

} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_UTILS_H
