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


#ifdef _mm512_stream_load_si512
#define width 64
//https://stackoverflow.com/questions/2963898/faster-alternative-to-memcpy
// https://squadrick.dev/journal/going-faster-than-memcpy.html
void fastMemcpy(void* pvDest, const void* pvSrc, size_t nBytes) {
  CHECK(nBytes % width == 0);
  CHECK((intptr_t(pvDest) & 63) == 0);
  CHECK((intptr_t(pvSrc) & 63) == 0);
  const __m512i* pSrc = reinterpret_cast<const __m512i*>(pvSrc);
  __m512i* pDest = reinterpret_cast<__m512i*>(pvDest);
  int64_t nVects = nBytes / sizeof(*pSrc);
  for (; nVects > 0; nVects--, pSrc++, pDest++) {
    const __m512i loaded = _mm512_stream_load_si512((void*)pSrc);
    _mm512_stream_si512(pDest, loaded);
  }
  _mm_sfence();
}
#else
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
#endif

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

void AsyncStreamCopy(__m256i* pDest, const __m256i* pSrc, int64_t nVects) {
  for (; nVects > 0; nVects--, pSrc++, pDest++) {
    const __m256i loaded = _mm256_stream_load_si256(pSrc);
    _mm256_stream_si256(pDest, loaded);
  }
}

void MultithreadStreamCopy(double* gpdOutput, const double* gpdInput, const int64_t cnDoubles) {
  CHECK((cnDoubles * sizeof(double)) % sizeof(__m256i) == 0);
  const uint32_t maxThreads = std::thread::hardware_concurrency();
  std::vector<std::thread> thrs;
  thrs.reserve(maxThreads + 1);

  const __m256i* pSrc = reinterpret_cast<const __m256i*>(gpdInput);
  __m256i* pDest = reinterpret_cast<__m256i*>(gpdOutput);
  const int64_t nVects = cnDoubles * sizeof(*gpdInput) / sizeof(*pSrc);

  for (uint32_t nThreads = 1; nThreads <= maxThreads; nThreads++) {
    auto start = std::chrono::high_resolution_clock::now();
    lldiv_t perWorker = div((long long)nVects, (long long)nThreads);
    int64_t nextStart = 0;
    for (uint32_t i = 0; i < nThreads; i++) {
      const int64_t curStart = nextStart;
      nextStart += perWorker.quot;
      if ((long long)i < perWorker.rem) {
        nextStart++;
      }
      thrs.emplace_back(AsyncStreamCopy, pDest + curStart, pSrc + curStart, nextStart - curStart);
    }
    for (uint32_t i = 0; i < nThreads; i++) {
      thrs[i].join();
    }
    _mm_sfence();
    auto elapsed = std::chrono::high_resolution_clock::now() - start;
    double nSec = 1e-6 * std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
    printf("Stream copy %d threads: %.3lf bytes/sec\n", (int)nThreads, cnDoubles * 2 * sizeof(double) / nSec);
    thrs.clear();
  }
}

} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_UTILS_H
