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
#include "arrow/io/file.h"
#include "parquet/stream_writer.h"

namespace surfingdb {
namespace table {

mtable::~mtable() {
  free((void*)payload);
  capacity = 0;
  key_dist->clear();
  key_groups->clear();
  placement_index->clear();
}

mtable::mtable(const std::shared_ptr<node> node_ptr, const std::shared_ptr<TableSchema> schema_ptr,
               size_t capacity) {
  CHECK_GE(capacity, 0);
  this->capacity = capacity;
  this->schema_ptr = schema_ptr;
  this->node_ptr = node_ptr;
  payload = (uint8_t*)aligned_alloc(64, capacity);
  CHECK(payload != NULL);
  schedule_size = -1;
  key_dist = std::make_unique<std::map<size_t, std::vector<std::pair<int, size_t>>, std::less<size_t>>>();
  key_groups = std::make_unique<std::map<size_t, std::vector<size_t>, std::less<size_t>>>();
  placement_index = std::make_unique<std::map<int, size_t>>();

  offset = 0;
  row_count = 0;
}

void mtable::flush_rma_memory(size_t rows) {
  key_dist->clear();
  key_groups->clear();
  placement_index->clear();
  std::string filepath = FLUSH_DIR + std::to_string(node_ptr->rank) + "-" + std::to_string(omp_get_thread_num()) + ".flush_rma_memory";
  this->flush(filepath, schedule, rows, rows * schema_ptr->rowSize());
  MPI_Free_mem(schedule);
  MPI_Win_free(&win);
  schedule_size = -1;
  this->load(filepath);
}

void mtable::copy_rma_memory(size_t rows) {
  reserveRow(rows);
  key_dist->clear();
  key_groups->clear();
  placement_index->clear();
  memcpy(payload_ptr(), schedule, schema_ptr->rowSize() * rows);
  MPI_Free_mem(schedule);
  MPI_Win_free(&win);
  schedule_size = -1;
  offset = rows * schema_ptr->rowSize();
  row_count = rows;
}

void mtable::reserve_rma_memory(size_t rows) {
  if (schedule_size != -1 && schedule_size < rows * schema_ptr->rowSize()) {
    MPI_Free_mem(schedule);
    MPI_Win_free(&win);
  }
  schedule_size = rows * schema_ptr->rowSize();
  MPI_Alloc_mem(schedule_size, MPI_INFO_NULL, &schedule); // allocate memory in temptable directly
  MPI_Win_create(schedule, schedule_size, schema_ptr->rowSize(), MPI_INFO_NULL, MPI_COMM_WORLD, &win);
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
std::unique_ptr<RowBuffer> mtable::readRow(int index) {
  CHECK_LT(index, row_count);
  CHECK_NOTNULL(schema_ptr);
  return std::make_unique<RowBuffer>(schema_ptr, &payload[schema_ptr->rowSize() * index]);
}

size_t mtable::placement(size_t key) {
  int base = node_ptr->world % 2 == 0 ? node_ptr->world - 1 : node_ptr->world;

  return key % base;
}

uint8_t* mtable::payload_ptr() {
  return &this->payload[0];
}

void mtable::appendRow(RowBuffer& row) {
  CHECK_EQ(row.schema_sig(), schema_ptr->signature()); // check schema signature
  CHECK_EQ(schema_ptr->rowSize(), row.row_size());     // check row size
  CHECK_LE(row.row_size() + offset, capacity);         // check capacity of temp table
  memcpy(&payload[offset], row.payload_ptr(), row.row_size());
  offset += row.row_size();
  row_count++;
  CHECK_LE(offset, capacity);
}

void mtable::verifyShuffle(const Field& field) {
  LOG(INFO) << "verify " << row_count << " rows" << omp_get_thread_num() << " " << node_ptr->rank;
  for (size_t i = 0; i < row_count; i++) {
    auto r = this->readRow(i);
    Value v;
    r->read(field, v);
    size_t key = value_hasher.operator()(v);
    CHECK_EQ(placement(key), node_ptr->rank);
  }
}

void mtable::group(const Field& f, bool local) {
  const int world = node_ptr->world;
  const int rank = node_ptr->rank;

  CHECK(schema_ptr->containField(f));

  key_groups->clear(); // reset key_groups
  key_dist->clear();   // reset key_dist

  for (size_t i = 0; i < row_count; i++) {
    auto r = readRow(i);
    Value v;
    r->read(f, v);
    size_t key = value_hasher.operator()(v);
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
  CHECK(!key_groups->empty());
  CHECK(!placement_index->empty());
  CHECK(dest < node_ptr->world);

  int index = placement_index->at(dest);
  return &this->payload[index * schema_ptr->rowSize()];
}

size_t mtable::range_row_size(int dest) {
  if (dest == node_ptr->world - 1) {
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
    size_t key = value_hasher.operator()(v);
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
  for (int i = 0; i < node_ptr->world; i++) {
    sender->placement_index->insert({ i, index });
    for (auto g : *key_groups) {
      size_t rank = partitioner(g.first, node_ptr->rank, node_ptr->world);
      /**
       * @brief if placment of a key equals to a specific rank i
       *
       */
      if (rank == (size_t)i) {
        for (auto item : g.second) {
          auto row = this->readRow(item);
          Value v;
          row->read(f, v);
          size_t key = value_hasher.operator()(v);
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

std::shared_ptr<TableSchema> mtable::getSchema() {
  return this->schema_ptr;
}

std::shared_ptr<arrow::Schema> mtable::getArrowSchema() {
  return this->schema_ptr->getArrowSchema();
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

std::shared_ptr<arrow::ArrayBuilder> getBuilder(const RowType::type& type) {
  if (type == RowType::BOOL) {
    return std::make_shared<arrow::BooleanBuilder>();
  }
  if (type == RowType::INT) {
    return std::make_shared<arrow::Int32Builder>();
  }
  if (type == RowType::LONG) {
    return std::make_shared<arrow::Int64Builder>();
  }
  if (type == RowType::DOUBLE) {
    return std::make_shared<arrow::DoubleBuilder>();
  }
  if (type == RowType::STRING) {
    return std::make_shared<arrow::StringBuilder>();
  }
  return std::make_shared<arrow::NullBuilder>();
}

arrow::Status append(arrow::ArrayBuilder* builder, const Field& field, const PValue& p_val, const Value& v) {
  if (field.type == RowType::BOOL) {
    ARROW_RETURN_NOT_OK(((arrow::BooleanBuilder*)builder)->Append(p_val.bool_val));
  }
  if (field.type == RowType::INT) {
    ARROW_RETURN_NOT_OK(((arrow::Int32Builder*)builder)->Append(p_val.int_val));
  }
  if (field.type == RowType::LONG) {
    ARROW_RETURN_NOT_OK(((arrow::Int64Builder*)builder)->Append(p_val.long_val));
  }
  if (field.type == RowType::DOUBLE) {
    ARROW_RETURN_NOT_OK(((arrow::DoubleBuilder*)builder)->Append(p_val.double_val));
  }
  if (field.type == RowType::STRING) {
    ARROW_RETURN_NOT_OK(((arrow::StringBuilder*)builder)->Append(p_val.string_val));
  }
  if (field.type == RowType::LIST) {
    auto list_builder = (arrow::ListBuilder*)builder;
    ARROW_RETURN_NOT_OK(list_builder->Append());

    auto e_builder = list_builder->value_builder();
    Value v;
    Field lfield;
    lfield.type = field.list_type;
    for (auto m : v.list_value) {
      ARROW_RETURN_NOT_OK(append(e_builder, lfield, m, v));
    }
  }
  if (field.type == RowType::MAP) {
    auto map_builder = (arrow::MapBuilder*)builder;
    ARROW_RETURN_NOT_OK(map_builder->Append());
    auto k_builder = map_builder->key_builder();
    auto v_builder = map_builder->value_builder();
    Value v;
    Field kfield, vfield;
    kfield.type = field.map_key_type;
    vfield.type = field.map_value_type;
    for (auto n : v.map_value) {
      ARROW_RETURN_NOT_OK(append(k_builder, kfield, n.first, v));
      ARROW_RETURN_NOT_OK(append(v_builder, vfield, n.second, v));
    }
  }
  return arrow::Status::OK();
}

std::shared_ptr<arrow::Table> mtable::getArrowTable() {
  return this->table_ptr;
}

arrow::Status mtable::toColumnar() {
  /**
   * Build list of builders to append
   */
  std::vector<std::shared_ptr<arrow::ArrayBuilder>> builders;
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrow::MemoryPool* pool = arrow::default_memory_pool();
  for (auto i = 0; i < this->getSchema()->fields.size(); i++) {
    auto type = this->getSchema()->fields.at(i).type;
    if (type == RowType::LIST) {
      auto keytype = this->getSchema()->fields.at(i).list_type;
      builders.push_back(std::make_shared<arrow::ListBuilder>(pool, getBuilder(keytype)));
      continue;
    }
    if (type == RowType::MAP) {
      auto keytype = this->getSchema()->fields.at(i).map_key_type;
      auto valuetype = this->getSchema()->fields.at(i).map_value_type;
      builders.push_back(std::make_shared<arrow::MapBuilder>(pool, getBuilder(keytype), getBuilder(valuetype)));
      continue;
    }
    builders.push_back(getBuilder(type));
  }

  /**
   * read each row and field value, append to corresponding builder, release mtable vector<RowBuffer>
   */
  for (auto j = 0; j < row_count; j++) {
    auto r = readRow(j);
    CHECK(getSchema()->fields.size() == builders.size());
    for (auto k = 0; k < getSchema()->fields.size(); k++) {
      auto field = getSchema()->fields.at(k);
      auto builder_ptr = builders.at(k);
      Value v;
      r->read(field, v);
      ARROW_RETURN_NOT_OK(append(builder_ptr.get(), field, v.p_val, v));
    }
  }

  for (auto b : builders) {
    std::shared_ptr<arrow::Array> _array;
    ARROW_RETURN_NOT_OK(b->Finish(&_array));
    arrays.push_back(_array);
  }
  this->table_ptr = arrow::Table::Make(this->getArrowSchema(), arrays);
  /**
   * release mtable vector
   */
  release();
  return arrow::Status::OK();
}

std::shared_ptr<TableSchema> mtable::getCompactSchema() {
  std::vector<size_t> max_units;
  auto compact_schema_ptr = std::make_shared<TableSchema>(*schema_ptr.get());
  /**
   * find list and map type max_unit per mtable rows
   */
  for (auto field : schema_ptr->fields) {

    if (field.list_type != RowType::VOID) {
      max_units.push_back(0); // max unit
      max_units.push_back(0); // max list unit
    }
    if (field.map_value_type != RowType::VOID) {
      max_units.push_back(0); // max unit
      max_units.push_back(0); // max key unit
      max_units.push_back(0); // max value unit
    }
    if (field.type == RowType::STRING) {
      max_units.push_back(0); // max unit
    }
  }

  for (size_t i = 0; i < row_count; i++) {
    size_t index = 0;
    auto r = this->readRow(i);
    for (auto field : schema_ptr->fields) {
      CHECK_GE(schema_ptr->_offsets->size(), 0);
      CHECK(schema_ptr->_offsets->find(field) != schema_ptr->_offsets->end());
      uint64_t offset = schema_ptr->_offsets->at(field);

      if (field.list_type != RowType::VOID) {
        std::vector<size_t> list_lens = r->readLen(field, offset);
        max_units.at(index) = max_units.at(index) < list_lens.at(0) ? list_lens.at(0) : max_units.at(index);
        max_units.at(index + 1) = max_units.at(index) < list_lens.at(1) ? list_lens.at(1) : max_units.at(index);
        index += 2;
      }
      if (field.map_value_type != RowType::VOID) {
        std::vector<size_t> map_lens = r->readLen(field, offset);
        max_units.at(index) = max_units.at(index) < map_lens.at(0) ? map_lens.at(0) : max_units.at(index);
        max_units.at(index + 1) = max_units.at(index) < map_lens.at(1) ? map_lens.at(1) : max_units.at(index);
        max_units.at(index + 2) = max_units.at(index) < map_lens.at(2) ? map_lens.at(2) : max_units.at(index);
        index += 3;
      }
      if (field.type == RowType::STRING) {
        std::vector<size_t> map_lens = r->readLen(field, offset);
        // leave extra space for \0
        max_units.at(index) = max_units.at(index) < map_lens.at(0) + 1 ? map_lens.at(0) + 1 : max_units.at(index);
        index += 1;
      }
    }
  }
  size_t global_units[max_units.size()];
  memcpy(global_units, &max_units[0], sizeof(size_t) * max_units.size());
  const int max_size = max_units.size();
  /**
   * use tag to support multiple threads calling allreduce caused confusion
   */
  if (node_ptr != nullptr) {
    if (node_ptr->rank == 0) {
      for (int i = 1; i < node_ptr->world; i++) {
        size_t local_units[max_units.size()];
        MPI_Recv(&local_units, max_size, MPI_UNSIGNED_LONG, i, omp_get_thread_num(), MPI_COMM_WORLD,
                 MPI_STATUS_IGNORE);
        for (int i = 0; i < max_size; i++) {
          global_units[i] = local_units[i] > global_units[i] ? local_units[i] : global_units[i];
        }
      }
      // broadcast with tag
      for (int i = 1; i < node_ptr->world; i++) {
        MPI_Send(&global_units, max_size, MPI_UNSIGNED_LONG, i, omp_get_thread_num(), MPI_COMM_WORLD);
      }
    } else {
      MPI_Send(&max_units[0], max_size, MPI_UNSIGNED_LONG, 0, omp_get_thread_num(), MPI_COMM_WORLD);
      MPI_Recv(&global_units, max_size, MPI_UNSIGNED_LONG, 0, omp_get_thread_num(), MPI_COMM_WORLD,
               MPI_STATUS_IGNORE);
    }
  }

  // MPI_Allreduce(&max_units[0], &global_units, max_size, MPI_UNSIGNED_LONG, MPI_MAX, MPI_COMM_WORLD);

  int index = 0;
  for (auto& field : compact_schema_ptr->fields) {
    if (field.list_type != RowType::VOID) {
      field.max_unit_size = global_units[index++];
      field.max_list_unit_size = global_units[index++];
    }
    if (field.map_value_type != RowType::VOID) {
      field.max_unit_size = global_units[index++];
      field.max_map_key_unit_size = global_units[index++];
      field.max_map_value_unit_size = global_units[index++];
    }
    if (field.type == RowType::STRING) {
      field.max_unit_size = global_units[index++];
    }
  }
  compact_schema_ptr->updateRowSize();
  return compact_schema_ptr;
}

std::shared_ptr<mtable> mtable::compactTable() {
  auto compact_schema_ptr = getCompactSchema();

  auto compact_table_ptr = std::make_shared<mtable>(node_ptr, compact_schema_ptr,
                                                    compact_schema_ptr->rowSize() * row_count);
  for (size_t index = 0; index < row_size(); index++) {
    auto r = readRow(index);
    auto rcompact = RowBuffer(compact_schema_ptr);
    for (auto f : schema_ptr->fields) {
      if (f.type != RowType::LIST && f.type != RowType::MAP && f.type != RowType::STRING) continue;
      Value v;
      r->read(f, v);
      for (auto f1 : compact_schema_ptr->fields) {
        if (f1 == f) {
          rcompact.write(f1, v);
        }
      }
    }
    compact_table_ptr->appendRow(rcompact);
  }
  CHECK_LE(compact_schema_ptr->rowSize(), schema_ptr->rowSize());
  LOG(INFO) << "reduced row size " << compact_schema_ptr->rowSize() << "v.s" << schema_ptr->rowSize();
  return compact_table_ptr;
}

void mtable::release() {
  key_dist->clear();
  key_groups->clear();
  placement_index->clear();
}

void mtable::appendRows(std::vector<std::shared_ptr<RowBuffer>>& rows) {
  for (std::shared_ptr<RowBuffer> m : rows) {
    appendRow(*m.get());
  }
}
} // namespace table
} // namespace surfingdb