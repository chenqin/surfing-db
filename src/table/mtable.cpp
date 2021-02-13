//
// Created by cq on 2/6/21.
//
#include "mtable.h"
#include <sys/uio.h>
#include <unistd.h>
#include "table.h"

namespace surfingdb {
namespace table {

mtable::~mtable() {
  payload.clear();
  payload.shrink_to_fit();
  key_dist->clear();
  key_groups->clear();
  placement_index->clear();
}

mtable::mtable(const std::shared_ptr<node> node_ptr, const std::shared_ptr<TableSchema> schema_ptr, size_t capacity) {
  CHECK_GT(capacity, 0);
  this->schema_ptr = schema_ptr;
  this->node_ptr = node_ptr;
  payload.resize(capacity);
  schedule_size = -1;
  key_dist = std::make_unique<std::map<size_t, std::vector<std::pair<int, size_t>>, std::less<size_t>>>();
  key_groups = std::make_unique<std::map<size_t, std::vector<size_t>, std::less<size_t>>>();
  placement_index = std::make_unique<std::map<int, size_t>>();

  offset = 0;
  row_count = 0;
}

void mtable::flush_rma_memory(size_t rows) {
  payload.clear();
  payload.shrink_to_fit();

  key_dist->clear();
  key_groups->clear();
  placement_index->clear();
  std::string filepath = FLUSH_DIR + std::to_string(node_ptr->rank) + ".flush_rma_memory";
  this->flush(filepath, schedule, rows, rows * schema_ptr->size());
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
  memcpy(payload_ptr(), schedule, schema_ptr->size() * rows);
  MPI_Free_mem(schedule);
  MPI_Win_free(&win);
  schedule_size = -1;
  offset = rows * schema_ptr->size();
  row_count = rows;
}

void mtable::reserve_rma_memory(size_t rows) {
  if (schedule_size != -1 && schedule_size < rows * schema_ptr->size()) {
    MPI_Free_mem(schedule);
    MPI_Win_free(&win);
  }
  schedule_size = rows * schema_ptr->size();
  MPI_Alloc_mem(schedule_size, MPI_INFO_NULL, &schedule); // allocate memory in temptable directly
  MPI_Win_create(schedule, schedule_size, schema_ptr->size(), MPI_INFO_NULL, MPI_COMM_WORLD, &win);
}

void mtable::reserveRow(size_t rows) {
  CHECK_GT(schema_ptr->size(), 0);
  CHECK_GE(rows, 0);
  if (this->payload.capacity() > rows * schema_ptr->size()) return;
  payload.resize(rows * schema_ptr->size());
}

/**
   * directly readRow from temptable memory
   * @param index
   * @return
   */
std::unique_ptr<RowBuffer> mtable::readRow(int index) {
  CHECK_LT(index, row_count);
  CHECK_NOTNULL(schema_ptr);
  return std::make_unique<RowBuffer>(schema_ptr, &payload[schema_ptr->size() * index]);
}

/**
  * find better hash function
  * @param key
  * @return
  */
size_t mtable::placement(size_t key) {
  int base = node_ptr->world % 2 == 0 ? node_ptr->world - 1 : node_ptr->world;
  return key % base;
}

uint8_t* mtable::payload_ptr() {
  return &this->payload[0];
}

void mtable::appendRow(RowBuffer& row) {
  CHECK_EQ(row.schema_sig(), schema_ptr->schema_sig());  //check schema signature
  CHECK_EQ(schema_ptr->size(), row.row_size());          //check row size
  CHECK_LE(row.row_size() + offset, payload.capacity()); // check capacity of temp table
  memcpy(&payload[offset], row.payload_ptr(), row.row_size());
  offset += row.row_size();
  row_count++;
  CHECK_LE(offset, payload.capacity());
}

void mtable::verify(const Field& field) {
  //LOG(INFO) << "verify " << row_count << " rows";
  for (size_t i = 0; i < row_count; i++) {
    auto r = this->readRow(i);
    Value v;
    r->read(field, v);
    size_t key = value_hasher.operator()(v);
    CHECK_EQ(placement(key), node_ptr->rank);
  }
}

void mtable::group(const Field& f) {
  auto start = MPI_Wtime();
  CHECK(schema_ptr->exist(f));
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
  /**
   * [hash_key, size, rank]
   */
  size_t key_count[key_groups->size()][3];
  auto it = key_groups->begin();
  int i = 0;
  while (it != key_groups->end()) {
    key_count[i][0] = it->first;
    key_count[i][1] = it->second.size();
    key_count[i][2] = node_ptr->rank;
    i++;
    it++;
  }
  /*
   * merge key_count array into one
   */
  size_t local_group_sizes[node_ptr->world];
  memset(local_group_sizes, 0, node_ptr->world * sizeof(size_t)); //always clear memory before use

  int recvcounts[node_ptr->world], displs[node_ptr->world];
  memset(recvcounts, 0, node_ptr->world * sizeof(int));
  memset(displs, 0, node_ptr->world * sizeof(int));

  displs[0] = 0;
  local_group_sizes[node_ptr->rank] = key_groups->size() * 3;

  size_t recv[node_ptr->world]; // type should be same as local_group_sizes
  MPI_Allreduce(&local_group_sizes, &recv, node_ptr->world, MPI_UNSIGNED_LONG, MPI_MAX, MPI_COMM_WORLD);

  std::vector<size_t> _g_groups; // keep key, row counts on each process
  size_t _global_group_size;     // apply to group_by

  _global_group_size = recv[0];
  recvcounts[0] = recv[0];
  for (i = 1; i < node_ptr->world; i++) {
    displs[i] = displs[i - 1] + recv[i - 1];
    _global_group_size += recv[i];
    recvcounts[i] = (int)recv[i];
  }

  CHECK_GE(_global_group_size, key_groups->size() * 3);
  _g_groups.resize(_global_group_size);

  MPI_Allgatherv(key_count, key_groups->size() * 3, MPI_UNSIGNED_LONG, &_g_groups[0], recvcounts, displs, MPI_UNSIGNED_LONG, MPI_COMM_WORLD);

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
  //LOG(INFO) << "group by costs " << MPI_Wtime() - start << " on " << node_ptr->rank;
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
  return &this->payload[index * schema_ptr->size()];
}

/**
   * sort rows based on destination process and update _start_index map
   */
void mtable::placement_sort(const Field& f) {
  auto start = MPI_Wtime();
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

  mtable sender(node_ptr, schema_ptr, row_count * schema_ptr->size());
  int index = 0;
  for (int i = 0; i < node_ptr->world; i++) {
    placement_index->insert({ i, index });
    for (auto g : *key_groups) {
      size_t placement = sender.placement(g.first);
      if (placement == (size_t)i) {
        for (auto item : g.second) {
          auto row = this->readRow(item);
          sender.appendRow(*row.get());
          index++;
        }
      }
    }
  }
  memcpy(&payload[0], sender.payload_ptr(), row_count * schema_ptr->size());
  //LOG(INFO) << "in place sort costs " << MPI_Wtime() - start << " on " << node_ptr->rank;
}

/**
   * append buffer to file in batch
   */
void mtable::flush(const std::string& path, uint8_t* ptr, size_t rows, size_t len) {
  //LOG(INFO) << "write to " << path;
  struct iovec iov[FILE_IO_VECTOR];
  ssize_t nr;
  int fd = open(path.c_str(), O_CREAT | O_RDWR, 0644);
  CHECK_NE(fd, -1);

  size_t sig = schema_ptr->schema_sig();
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
  CHECK_EQ(sig, schema_ptr->schema_sig());
  CHECK_GE(::read(fd, &offset, sizeof(size_t)), 0);
  CHECK_GE(::read(fd, &row_count, sizeof(size_t)), 0);
  CHECK_GE(row_count, 0);

  if (row_count == 0) {
    payload.clear();
    payload.shrink_to_fit();
  } else {
    CHECK_EQ(offset / row_count, schema_ptr->size());
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
  //LOG(INFO) << "load from " << path;
  close(fd);
}
size_t mtable::row_size() {
  return this->row_count;
}
std::shared_ptr<TableSchema> mtable::getSchema() {
  return this->schema_ptr;
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
} // namespace table
} // namespace surfingdb