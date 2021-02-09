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
  std::vector<std::vector<uint8_t>> chunks;
  int page_size;
  int loaded_page;
  size_t offset;
public:
  mchunk() {
    page_size = 0;
    loaded_page = 0;
    offset = 0;
  }
  void clear() {
    for(auto p : chunks) {
      p.clear();
      p.shrink_to_fit();
    }
    chunks.clear();
    chunks.shrink_to_fit();
    page_size = 0;
    loaded_page = 0;
    offset = 0;
  }
  ~mchunk() {
    clear();
  }

  /**
   * directly read from temptable memory
   * @param index
   * @return
   */
  std::unique_ptr<RowBuffer> read(std::shared_ptr<TableSchema> schema_ptr, int row_index) {
    int row_per_page = SSD_CHUNK_SIZ/schema_ptr->size();
    int page_index = row_index/row_per_page;
    int page_internal_offset = row_index - row_per_page*page_index;
    std::vector<uint8_t> page_ptr = chunks[page_index];
    CHECK_LE(page_internal_offset, SSD_CHUNK_SIZ- schema_ptr->size());
    return std::make_unique<RowBuffer>(schema_ptr, &page_ptr[page_internal_offset]);
  }

  int get_page_size() {
    return page_size;
  }

  void append(RowBuffer& row) {
    if(page_size == 0) {
      std::vector<uint8_t> temp(SSD_CHUNK_SIZ);
      page_size = 1;
      loaded_page = 0;
      offset = 0;
      chunks.push_back(temp);
    }
    CHECK_GE(page_size, 1);
    CHECK_LE(offset, SSD_CHUNK_SIZ);
    CHECK_GE(offset, 0);
    if (row.size() + offset > SSD_CHUNK_SIZ) {
      std::vector<uint8_t> temp(SSD_CHUNK_SIZ, 0);
      page_size++;
      chunks.push_back(temp);
      loaded_page++;
      offset = 0;
    }
    LOG(INFO) << "append to " << page_size-1 << " @ " << offset;
    std::vector<uint8_t> ptr = chunks[page_size-1];
    memcpy(reinterpret_cast<void*>(&ptr[offset]), row.payload_ptr(), row.size());
    offset += row.size();
  }
};
} // namespace table
} // namespace surfingdb