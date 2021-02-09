//
// Created by cq on 2/6/21.
//
#include "mtable.h"
#include "table.h"

namespace surfingdb {
namespace table {

mtable::~mtable() {
  MPI_Win_free(&win);
  MPI_Free_mem(schedule); // gc memory in temptable
  payload.clear();
  payload.shrink_to_fit();
}

mtable::mtable(const std::shared_ptr<Node> node_ptr, const std::shared_ptr<TableSchema> schema_ptr) {
  this->schema_ptr = schema_ptr;
  this->node_ptr = node_ptr;
  payload.resize(MEM_PAGE_SIZE);

  size_t row_size = schema_ptr->size();
  MPI_Alloc_mem(MEM_PAGE_SIZE, MPI_INFO_NULL, &schedule); // allocate memory in temptable directly
  MPI_Win_create(schedule, MEM_PAGE_SIZE, row_size, MPI_INFO_NULL, MPI_COMM_WORLD, &win);
  schedule_size = MEM_PAGE_SIZE;
  key_dist = std::make_unique<std::map<size_t, std::vector<std::pair<int, size_t>>, std::less<size_t>>>();
  key_groups = std::make_unique<std::map<size_t, std::vector<size_t>, std::less<size_t>>>();
  chunk_ptr = std::make_unique<mchunk>();
  placement_index = std::make_unique<std::map<int, size_t>>();

  offset = 0;
  row_count = 0;
}

void mtable::flush_rma_memory(size_t rows) {
  reserve_table(rows);
  key_dist->clear();
  key_groups->clear();
  placement_index->clear();
  memcpy(payload_ptr(), schedule, schema_ptr->size() * rows);
  chunk_ptr->clear();

  for(int i = 0 ; i < rows ; i++) {
    RowBuffer r(schema_ptr, schedule + i*schema_ptr->size());
    chunk_ptr->append(r);
  }
  offset = rows * schema_ptr->size();
  row_count = rows;
}

void mtable::reserve_rma_memory(size_t rows) {
  if (rows * schema_ptr->size() < schedule_size) return;
  int page = (rows * schema_ptr->size()) / MEM_PAGE_SIZE + ((rows * schema_ptr->size()) % MEM_PAGE_SIZE != 0) ? 1 : 0;
  MPI_Win_free(&win);
  MPI_Free_mem(schedule);                                        // gc memory in temptable
  MPI_Alloc_mem(page * MEM_PAGE_SIZE, MPI_INFO_NULL, &schedule); // allocate memory in temptable directly
  int max_row_size = page * MEM_PAGE_SIZE / schema_ptr->size();
  MPI_Win_create(schedule, page * MEM_PAGE_SIZE, max_row_size, MPI_INFO_NULL, MPI_COMM_WORLD, &win);
  schedule_size = page * MEM_PAGE_SIZE;
}

void mtable::reserve_table(size_t rows) {
  CHECK_GT(schema_ptr->size(), 0);
  if (this->payload.capacity() > rows * schema_ptr->size()) return;
  int page = (rows * schema_ptr->size()) / MEM_PAGE_SIZE + ((rows * schema_ptr->size()) % MEM_PAGE_SIZE != 0) ? 1 : 0;
  payload.resize(page * MEM_PAGE_SIZE);
}

/**
   * directly read from temptable memory
   * @param index
   * @return
   */
std::unique_ptr<RowBuffer> mtable::read(int index) {
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

void mtable::ingest(RowBuffer& row) {
  CHECK_EQ(row.schema_sig(), schema_ptr->schema_sig()); //check schema signature
  CHECK_EQ(schema_ptr->size(), row.size());             //check row size
  CHECK_LE(row.size() + offset, payload.capacity());    // check capacity of temp table
  memcpy(&payload[offset], row.payload_ptr(), row.size());
  offset += row.size();
  row_count++;
  chunk_ptr->append(row);
  CHECK_LE(offset, payload.capacity());
}

void mtable::verify(const Field& field) {
  //LOG(INFO) << "verify " << row_count << " rows";
  for (size_t i = 0; i < row_count; i++) {
    auto r = read(i);
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
    auto r = read(i);
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
  node_ptr->forward();
  //LOG(INFO) << "group by costs " << MPI_Wtime() - start << " on " << node_ptr->rank;
}

void mtable::shuffle(const Field& f) {
  CHECK(schema_ptr->exist(f));
  group(f);
  placement_sort(f); // experiment shows batching still make sense with 10% overhead

  auto start = MPI_Wtime();
  MPI_Aint recv_buffer_len = 0;
  size_t expected_rows[node_ptr->world], expected_start_index[node_ptr->world];
  memset(expected_rows, 0, node_ptr->world * sizeof(size_t));
  memset(expected_start_index, 0, node_ptr->world * sizeof(size_t));

  for (auto k : *key_dist) {
    int place = placement(k.first);
    if (place == node_ptr->rank) {
      for (auto p : k.second) {
        recv_buffer_len += p.second;
        expected_rows[p.first] += p.second;
      }
    }
  }

  for (int i = 1; i < node_ptr->world; i++) {
    expected_start_index[i] = expected_start_index[i - 1] + expected_rows[i - 1];
  }
  // LOG(INFO) << "start exchange index " << node_ptr->rank;
  size_t target_disp[node_ptr->world]; // from each process, tell others start index of assigned memory
  for (int i = 0; i < node_ptr->world; i++) {
    size_t start_index;
    MPI_Scatter(expected_start_index, 1, MPI_UNSIGNED_LONG, &start_index, 1, MPI_UNSIGNED_LONG, i, MPI_COMM_WORLD);
    target_disp[i] = start_index;
  }
  reserve_rma_memory(recv_buffer_len);

  MPI_Win_fence(0, win);
#pragma omp parallel for shared(schedule)
  for (int dest = 0; dest < node_ptr->world; dest++) {
    uint8_t* rangePtr = this->range_ptr(dest);
    int index = (int)target_disp[dest];
    int count = 0;
    if (dest != node_ptr->world - 1) {
      count = placement_index->at(dest + 1) - placement_index->at(dest);
    } else {
      count = row_count - placement_index->at(dest);
    }
    MPI_Win_lock(MPI_LOCK_SHARED, dest, 0, win);
    MPI_Put(rangePtr, count, *(schema_ptr->getType()), dest, index, count, *(schema_ptr->getType()), win);
    MPI_Win_unlock(dest, win);
  }
  MPI_Win_fence(0, win);
  flush_rma_memory(recv_buffer_len);
  LOG(INFO) << "shuffle put costs " << MPI_Wtime() - start << " on " << node_ptr->rank;
}

/**
   * after in place sort, return starting addr of rows should place on dest
   * @param dest rank of process
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
    auto r = read(i);
    Value v;
    r->read(f, v);
    size_t key = value_hasher.operator()(v);
    if (key_groups->find(key) == key_groups->end()) {
      std::vector<size_t> arr;
      key_groups->insert({ key, arr });
    }
    key_groups->at(key).emplace_back(i);
  }

  TempTable sender(node_ptr, schema_ptr);
  auto temp_ptr = std::make_unique<mchunk>();
  sender.payload.reserve(row_count * schema_ptr->size());
  int index = 0;
  for (int i = 0; i < node_ptr->world; i++) {
    placement_index->insert({ i, index });
    for (auto g : *key_groups) {
      size_t placement = sender.decidePlacement(g.first);
      if (placement == (size_t)i) {
        for (auto item : g.second) {
          auto row = this->read(item);
          Value v;
          row->read(f, v);
          sender.ingest(*row.get());
          temp_ptr->append(*row.get());
          index++;
        }
      }
    }
  }
  chunk_ptr = std::move(temp_ptr);
  memcpy(&payload[0], sender.payload_ptr(), row_count * schema_ptr->size());
  //LOG(INFO) << "in place sort costs " << MPI_Wtime() - start << " on " << node_ptr->rank;
}
} // namespace table
} // namespace surfingdb