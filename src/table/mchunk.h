//
// Created by cq on 2/8/21.
//
#include <omp.h>
#include <vector>
#include "row.h"

#pragma once

namespace surfingdb {
namespace table {
/**
 * ssd friendly memory chunks
 */
class mchunk {
private:
  std::vector<uint8_t*> chunks;
  int page_size;
  int loaded_page;
  size_t offset;
  uint8_t* payload;
public:
  mchunk() {
    payload = static_cast<uint8_t*>(malloc(SSD_CHUNK_SIZ));
    page_size = 1;
    loaded_page = 0;
    offset = 0;
    chunks.push_back(payload);
  }
  ~mchunk() {
    for (auto p : chunks) {
      free(p);
    }
    page_size = 0;
    loaded_page = 0;
    offset = 0;
  }

  int get_page_size() {
    return page_size;
  }

  uint8_t* load(int page_index) {
    CHECK_LE(page_index, page_size);
    return chunks[page_index];
  }

  void append(RowBuffer& row) {
    if (row.size() + offset > SSD_CHUNK_SIZ) {
      uint8_t* newpayload = static_cast<uint8_t*>(malloc(SSD_CHUNK_SIZ));
      page_size++;
      chunks.push_back(newpayload);
      loaded_page++;
      offset = 0;
      payload = newpayload;
    }
    memcpy(payload + offset, row.payload_ptr(), row.size());
    offset += row.size();
  }
};
} // namespace table
} // namespace surfingdb