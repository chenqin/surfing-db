/*
 * Copyright Chen Qin on 12/30/20.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "mtable.h"
#include <math.h> /* isnan, sqrt */
#include <sys/uio.h>
#include <unistd.h>
#include "KMeanOperator.h"
#include "arrow/io/file.h"
#include "parquet/stream_writer.h"
#include "xgbop.h"

namespace surfingdb {
namespace table {

mtable::~mtable() {
  release();
  if (node_ptr != nullptr) MPI_Free_mem(payload);
  if (node_ptr == nullptr) free(payload);
}

mtable::mtable(const std::shared_ptr<mschema> schema_ptr, size_t capacity) {
  CHECK_GE(capacity, 0);
  this->capacity = capacity;
  this->schema_ptr = schema_ptr;
  this->node_ptr = nullptr;
  CHECK(sizeof(uint8_t) == sizeof(char));
  payload = (uint8_t*)malloc(capacity * sizeof(uint8_t));
  schedule_size = -1;
  key_dist = std::make_unique<std::map<size_t, std::vector<std::pair<int, size_t>>, std::less<size_t>>>();
  key_groups = std::make_unique<std::map<size_t, std::vector<size_t>, std::less<size_t>>>();
  placement_index = std::make_unique<std::map<int, size_t>>();

  offset = 0;
  row_count = 0;
}

mtable::mtable(const std::shared_ptr<node> node_ptr, const std::shared_ptr<mschema> schema_ptr,
               size_t capacity) {
  CHECK_GE(capacity, 0);
  this->capacity = capacity;
  this->schema_ptr = schema_ptr;
  this->node_ptr = node_ptr;
  CHECK(sizeof(uint8_t) == sizeof(char));
  if (node_ptr == nullptr) {
    rank = 0;
    world = 1;
  } else {
    rank = node_ptr->rank;
    world = node_ptr->world;
  }
  if (node_ptr != nullptr) MPI_Alloc_mem(capacity * sizeof(uint8_t), MPI_INFO_NULL, &payload);
  if (node_ptr == nullptr) payload = (uint8_t*)malloc(capacity * sizeof(uint8_t));
  schedule_size = -1;
  key_dist = std::make_unique<std::map<size_t, std::vector<std::pair<int, size_t>>, std::less<size_t>>>();
  key_groups = std::make_unique<std::map<size_t, std::vector<size_t>, std::less<size_t>>>();
  placement_index = std::make_unique<std::map<int, size_t>>();

  offset = 0;
  row_count = 0;
}

void mtable::reserveRow(size_t rows) {
  CHECK_GT(schema_ptr->rowSize(), 0);
  CHECK_GE(rows, 0);
  payload = (uint8_t*)realloc((void*)payload, rows * schema_ptr->rowSize());
}

/**
 * directly readRow from temptable memory
 * @param index
 * @return
 */
std::unique_ptr<mrow> mtable::readRow(int index) {
  CHECK_LT(index, row_count);
  CHECK_NOTNULL(schema_ptr);
  return std::make_unique<mrow>(schema_ptr, &payload[schema_ptr->rowSize() * index]);
}

size_t mtable::placement(size_t key) {
  int base = node_ptr->world % 2 == 0 ? node_ptr->world - 1 : node_ptr->world;

  return key % base;
}

uint8_t* mtable::payload_ptr() {
  return &this->payload[0];
}

void mtable::appendRow(mrow& row) {
  CHECK_EQ(row.schema_sig(), schema_ptr->signature()); // check schema signature
  CHECK_EQ(schema_ptr->rowSize(), row.capacity());     // check row size
  CHECK_LE(row.capacity() + offset, capacity);         // check capacity of temp table
  memcpy(&payload[offset], row.payload_ptr(), row.capacity());
  offset += row.capacity();
  row_count++;
  CHECK_LE(offset, capacity);
}

void mtable::verifyShuffle(const Field& field, std::function<size_t(size_t, int, int)> partitioner) {
  CHECK(world > 0);
  CHECK_LT(rank, world);
  for (size_t i = 0; i < row_count; i++) {
    auto r = this->readRow(i);
    Value v;
    r->read(field, v);
    size_t key = value_hasher.hash_value(field, v);
    CHECK_EQ(partitioner(key, node_ptr->rank, node_ptr->world), node_ptr->rank);
  }
}

void mtable::group(const Field& f, bool local) {
  CHECK(schema_ptr->containField(f));

  key_groups->clear(); // reset key_groups
  key_dist->clear();   // reset key_dist

  for (size_t i = 0; i < row_count; i++) {
    auto r = readRow(i);
    Value v;
    r->read(f, v);
    size_t key = value_hasher.hash_value(f, v);
    if (key_groups->find(key) == key_groups->end()) {
      std::vector<size_t> arr;
      key_groups->insert({ key, arr });
    }
    key_groups->at(key).emplace_back(i);
  }
  if (local) return;
  /**
   * [hash_key, size, rank]
   */
  size_t key_count[key_groups->size()][3];
  auto it = key_groups->begin();
  int i = 0;
  while (it != key_groups->end()) {
    key_count[i][0] = it->first;
    key_count[i][1] = it->second.size();
    key_count[i][2] = rank;
    i++;
    it++;
  }
  /*
   * merge key_count array into one
   */
  size_t local_group_sizes[world];
  memset(local_group_sizes, 0, world * sizeof(size_t)); // always clear memory before use

  int recvcounts[world], displs[world];
  memset(recvcounts, 0, world * sizeof(int));
  memset(displs, 0, world * sizeof(int));

  displs[0] = 0;
  local_group_sizes[rank] = key_groups->size() * 3;

  size_t recv[world]; // type should be same as local_group_sizes
  memcpy(recv, local_group_sizes, world * sizeof(size_t));

  MPI_Allreduce(&local_group_sizes, &recv, world, MPI_UNSIGNED_LONG, MPI_MAX, MPI_COMM_WORLD);

  std::vector<size_t> _g_groups; // keep key, row counts on each process
  size_t _global_group_size;     // apply to group_by

  _global_group_size = recv[0];
  recvcounts[0] = recv[0];
  for (i = 1; i < world; i++) {
    displs[i] = displs[i - 1] + recv[i - 1];
    _global_group_size += recv[i];
    recvcounts[i] = (int)recv[i];
  }

  CHECK_GE(_global_group_size, key_groups->size() * 3);
  _g_groups.resize(_global_group_size);

  MPI_Allgatherv(key_count, key_groups->size() * 3, MPI_UNSIGNED_LONG, &_g_groups[0], recvcounts, displs,
                 MPI_UNSIGNED_LONG, MPI_COMM_WORLD);

  // build key_hash to node and row count map
  for (size_t j = 0; j < _global_group_size; j += 3) {
    size_t key = _g_groups.at(j);
    size_t count = _g_groups.at(j + 1);
    size_t rank = _g_groups.at(j + 2);

    std::pair<int, size_t> pair(rank, count);
    if (key_dist->find(key) == key_dist->end()) {
      std::vector<std::pair<int, size_t>> vector;
      vector.emplace_back(pair);
      key_dist->insert({ key, vector });
    } else {
      key_dist->at(key).emplace_back(pair);
    }
  }
}

/**
 * after in place sort, return starting addr map rows should place on dest
 * @param dest rank map process
 * @return
 */
uint8_t* mtable::range_ptr(int dest) {
  CHECK(!placement_index->empty());
  CHECK(dest < world);
  CHECK(placement_index->find(dest) != placement_index->end());
  int index = placement_index->at(dest);
  return &this->payload[index * schema_ptr->rowSize()];
}

size_t mtable::range_row_size(int dest) {
  if (dest == world - 1) {
    return row_count - placement_index->at(dest);
  } else {
    return placement_index->at(dest + 1) - placement_index->at(dest);
  }
}

/**
 * sort rows based on destination process and update _start_index map
 */
std::shared_ptr<mtable> mtable::placement_sort(const Field& f, std::function<size_t(size_t key, int rank, int world)> partitioner) {
  // auto start = MPI_Wtime();
  placement_index->clear();
  key_groups->clear();

  for (size_t i = 0; i < row_count; i++) {
    auto r = readRow(i);
    Value v;
    r->read(f, v);
    size_t key = value_hasher.hash_value(f, v);

    // std::cout << "key " << key << " val " << v.p_val.int_val << std::endl;

    if (key_groups->find(key) == key_groups->end()) {
      std::vector<size_t> arr;
      key_groups->insert({ key, arr });
    }
    key_groups->at(key).emplace_back(i);
  }
  /***
   * build another table and sort rows based on field key placement to rank
   */
  auto sender = std::make_shared<mtable>(node_ptr, schema_ptr, row_count * schema_ptr->rowSize());

  int index = 0;
  for (int i = 0; i < world; i++) {
    sender->placement_index->insert({ i, index });
    for (auto g : *key_groups) {
      size_t placement = partitioner(g.first, rank, world);
      CHECK_LT(placement, world);
      /**
       * @brief if placment of a key equals to a specific rank i
       *
       */
      if (placement == (size_t)i) {
        for (auto item : g.second) {
          // std::cout << "place to rank " << placement << " with key " << g.first << " from row " << item << std::endl;
          CHECK_LT(item, row_count);
          auto row = this->readRow(item);
          Value v;
          row->read(f, v);
          size_t key = value_hasher.hash_value(f, v);
          if (sender->key_groups->find(key) == sender->key_groups->end()) {
            std::vector<size_t> arr;
            sender->key_groups->insert({ key, arr });
          }
          sender->key_groups->at(key).emplace_back(index);
          sender->appendRow(*row.get());
          index++;
        }
      }
    }
  }
  return sender;
}

/**
 * append buffer to file in batch
 */
void mtable::flush(const std::string& path, uint8_t* ptr, size_t rows, size_t len) {
  // LOG(INFO) << "write to " << path;
  struct iovec iov[FILE_IO_VECTOR];
  ssize_t nr;
  int fd = open(path.c_str(), O_CREAT | O_RDWR, 0644);
  CHECK_NE(fd, -1);

  size_t sig = schema_ptr->signature();
  CHECK_GE(::write(fd, &sig, sizeof(size_t)), 0);
  CHECK_GE(::write(fd, reinterpret_cast<const void*>(&len), sizeof(size_t)), 0);
  CHECK_GE(::write(fd, reinterpret_cast<const void*>(&rows), sizeof(size_t)), 0);

  size_t index = 0;
  /* fill out three iovec structures */
  while (index < len) {
    int batch = 0;
    for (int i = 0; i < FILE_IO_VECTOR && index < len; i++) {
      iov[i].iov_base = ptr + index;
      size_t payload_size = index + SSD_CHUNK_SIZ < len ? SSD_CHUNK_SIZ : len - index;
      iov[i].iov_len = payload_size;
      index += payload_size;
      batch++;
    }
    /* with a single call, entire batch */
    nr = writev(fd, iov, batch);
    CHECK_NE(nr, -1);
  }
  close(fd);
}

void mtable::load(const std::string& path) {
  struct iovec iov[FILE_IO_VECTOR];
  ssize_t nr;
  int fd = open(path.c_str(), O_RDONLY);
  CHECK_NE(fd, -1);

  size_t sig = 0;
  CHECK_GE(::read(fd, &sig, sizeof(size_t)), 0);
  CHECK_EQ(sig, schema_ptr->signature());
  CHECK_GE(::read(fd, &offset, sizeof(size_t)), 0);
  CHECK_GE(::read(fd, &row_count, sizeof(size_t)), 0);
  CHECK_GE(row_count, 0);

  if (row_count == 0) {
    payload = (uint8_t*)realloc((void*)payload, 0);
  } else {
    CHECK_EQ(offset / row_count, schema_ptr->rowSize());
    this->reserveRow(row_count);
  }

  size_t index = 0;
  /* fill out three iovec structures */
  while (index < offset) {
    int batch = 0;
    for (int i = 0; i < FILE_IO_VECTOR && index < offset; i++) {
      iov[i].iov_base = &payload[index];
      size_t payload_size = index + SSD_CHUNK_SIZ < offset ? SSD_CHUNK_SIZ : offset - index;
      iov[i].iov_len = payload_size;
      index += payload_size;
      batch++;
    }
    /* with a single call, entire batch */
    nr = readv(fd, iov, batch);
    CHECK_NE(nr, -1);
  }
  // LOG(INFO) << "load from " << path;
  close(fd);
}

size_t mtable::row_size() {
  return this->row_count;
}

std::shared_ptr<mschema> mtable::getSchema() {
  return this->schema_ptr;
}

void mtable::print() {
  if (this->row_count == 0) return;
  CHECK(this->row_count > 0);
  CHECK(this->schema_ptr->fields.size() > 0);
  auto r = this->readRow(0);
  Value v;
  r->read(schema_ptr->fields.at(0), v);
}

/**
 * for xgboost, we need to extract list of numerical fields and pass as array map float
 * @param fields features
 * @param data pointer to external float array
 */
void mtable::readFields(std::vector<Field> fields, float* data) {
  CHECK_NOTNULL(data);
  CHECK_GT(fields.size(), 0);

  for (const auto& f : fields) {
    CHECK_EQ(f.type, RowType::DOUBLE);
  }

  size_t columns = fields.size();
  memset(data, 0, sizeof(float) * columns * row_count);
  for (size_t i = 0; i < row_count; i++) {
    const auto row = readRow(i);
    for (size_t j = 0; j < columns; j++) {
      Value v;
      row->read(fields[j], v);
      data[i * columns + j] = (DOUBLE_TYPE)v.p_val.double_val;
    }
  }
}

void mtable::readField(const Field& field, float* data) {
  CHECK_NOTNULL(data);
  CHECK_EQ(field.type, RowType::DOUBLE);
  memset(data, 0, sizeof(DOUBLE_TYPE) * row_count);
  for (size_t i = 0; i < row_count; i++) {
    const auto row = readRow(i);
    Value v;
    row->read(field, v);
    data[i] = (DOUBLE_TYPE)v.p_val.double_val;
  }
}

void mtable::writeField(const Field& field, const float* data) {
  CHECK_NOTNULL(data);
  CHECK_EQ(field.type, RowType::DOUBLE);
  for (size_t i = 0; i < row_count; i++) {
    const auto row = readRow(i);
    Value v;
    v.p_val.double_val = (DOUBLE_TYPE)data[i];
    row->write(field, v);
  }
}

std::shared_ptr<node> mtable::getNodePtr() {
  return this->node_ptr;
}

void mtable::release() {
  key_dist->clear();
  key_groups->clear();
  placement_index->clear();
}

void mtable::appendRows(std::vector<std::shared_ptr<mrow>>& rows) {
  for (std::shared_ptr<mrow> m : rows) {
    appendRow(*m.get());
  }
}

void mtable::build_window() {
  MPI_Aint window_size;
  window_size = this->capacity;
  CHECK(MPI_Win_create(payload, window_size, sizeof(char), node_ptr->info, MPI_COMM_WORLD, &win) == 0);
}

void mtable::release_window() {
  if (win != MPI_WIN_NULL) MPI_Win_free(&win);
}

} // namespace table
} // namespace surfingdb