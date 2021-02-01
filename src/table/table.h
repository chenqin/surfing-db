//
// Created by Chen Qin on 12/31/20.
//

#ifndef SURFINGDB_TABLE_H
#define SURFINGDB_TABLE_H

#include <cstdarg>
#include <fcntl.h>
#include <future>
#include <math.h>
#include <mpi.h>
#include <omp.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include "KMeanOperator.h"
#include "Node.h"
#include "XGBOperator.h"
#include "row.h"
#include "table/gen-cpp/schema_constants.h"
#include "table/gen-cpp/schema_types.h"

#pragma once

namespace surfingdb {
namespace table {
using surfingdb::node::Node;

/**
         * use class operator+ to get row count of a key
         * @tparam T
         * @param a
         * @param b
         * @param len
         */
template <class T, auto op>
void reducer(void* a, void* b, int* len, MPI_Datatype*) {
  T* aa = static_cast<T*>(a);
  T* bb = static_cast<T*>(b);
  //#pragma omp simd
  for (int i = 0; i < *len; ++i) {
    bb[i] = op(aa[i], bb[i]);
  }
  //#pragma omp barrier
}

/**
 * stores vector of RowBuffer with same schema
 */
class TempTable {
private:
  // defines the node row table bind to
  std::shared_ptr<Node> ptr;
  std::shared_ptr<TableSchema> schema_ptr;
  std::vector<uint8_t> _payload;
   //partition field
  std::unique_ptr<std::map<size_t, std::vector<std::pair<int, size_t>>, std::less<size_t>>> _key_dist; // key hash and per node counts
  std::unique_ptr<std::map<size_t, std::vector<size_t>, std::less<size_t>>> _groups;                   // local key, offsets map

  size_t _count = 0; // number of rows in table
  size_t _pending = 0;
  size_t offset = 0; //current offset position

  struct iovec iov[FILE_IO_VECTOR];
  ssize_t nr;
  int fd;

public:
  TempTable(const std::shared_ptr<TableSchema> sharedPtr) {
    this->schema_ptr = sharedPtr;
    offset = 0;
    _payload.resize(MEM_PAGE_SIZE);
    _count = 0;
    _key_dist = std::make_unique<std::map<size_t, std::vector<std::pair<int, size_t>>, std::less<size_t>>>();
    _groups = std::make_unique<std::map<size_t, std::vector<size_t>, std::less<size_t>>>();
    LOG(INFO) << "for testing only";
  }
  TempTable(const std::shared_ptr<Node> node, const std::shared_ptr<TableSchema> sharedPtr) {
    this->schema_ptr = sharedPtr;
    _payload.resize(MEM_PAGE_SIZE);
    offset = 0;
    _count = 0;
    _key_dist = std::make_unique<std::map<size_t, std::vector<std::pair<int, size_t>>, std::less<size_t>>>();
    _groups = std::make_unique<std::map<size_t, std::vector<size_t>, std::less<size_t>>>();
    this->ptr = node;
  }

  ~TempTable() {
    clear();
  }

  void clear() {
    _payload.clear();
    offset = 0;
    _pending = 0;
    _count = 0;
    _groups->clear();
    _key_dist->clear();
  }

  void complete() {
    this->_count = _pending;
    _pending = 0;
  }

  uint8_t* payload_ptr() {
    return &this->_payload[0];
  }

  void ingest(RowBuffer row) {
    CHECK_EQ(row.schema_sig(), schema_ptr->schema_sig());     //check schema signature
    CHECK_EQ(schema_ptr->size(), row.size());                 //check row size
    CHECK_LT(row.size() + offset, _payload.max_size());       // check capacity of temp table
    memcpy(&_payload[offset], row.payload_ptr(), row.size()); // TODO(chenqin): should use fastcopy
    offset += row.size();
    _count++;

    // expand table size if needed
    if (offset >= MEM_PAGE_SIZE - row.size()) {
      _payload.resize(_payload.size() + MEM_PAGE_SIZE);
    }
  }

  /**
   * write buffer to file in batch
   */
  void flush(const std::string& path) {
    fd = open(path.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
    CHECK_NE(fd, -1);

    size_t sig = schema_ptr->schema_sig();
    CHECK_GE(::write(fd, &sig, sizeof(size_t)), 0);
    CHECK_GE(::write(fd, reinterpret_cast<const void*>(&offset), sizeof(size_t)), 0);
    CHECK_GE(::write(fd, reinterpret_cast<const void*>(&_count), sizeof(size_t)), 0);

    size_t index = 0;
    /* fill out three iovec structures */
    while (index < offset) {
      int batch = 0;
      for (int i = 0; i < FILE_IO_VECTOR && index < offset; i++) {
        iov[i].iov_base = &_payload[index];
        size_t payload_size = index + SSD_CHUNK_SIZ < offset ? SSD_CHUNK_SIZ : offset - index;
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

  void load(const std::string& path) {
    fd = open(path.c_str(), O_RDONLY);
    CHECK_NE(fd, -1);

    size_t sig = 0;
    CHECK_GE(::read(fd, &sig, sizeof(size_t)), 0);
    CHECK_EQ(sig, schema_ptr->schema_sig());
    CHECK_GE(::read(fd, &offset, sizeof(size_t)), 0);
    CHECK_GE(::read(fd, &_count, sizeof(size_t)), 0);
    CHECK_EQ(offset / _count, schema_ptr->size());
    _payload.resize(offset);

    size_t index = 0;
    /* fill out three iovec structures */
    while (index < offset) {
      int batch = 0;
      for (int i = 0; i < FILE_IO_VECTOR && index < offset; i++) {
        iov[i].iov_base = &_payload[index];
        size_t payload_size = index + SSD_CHUNK_SIZ < offset ? SSD_CHUNK_SIZ : offset - index;
        iov[i].iov_len = payload_size;
        index += payload_size;
        batch++;
      }
      /* with a single call, entire batch */
      nr = readv(fd, iov, batch);
      CHECK_NE(nr, -1);
    }
    LOG(INFO) << "load from " << path;
    close(fd);
  }

  /**
   * send entire column f to dest
   * @param dest
   * @param f
   * @param count
   * @param request
   */
  void async_send(int dest, const Field& f, MPI_Request& request) {
    MPI_Send(&_count, 1, MPI_UNSIGNED_LONG, dest, omp_get_thread_num(), MPI_COMM_WORLD);
    MPI_Isend(&_payload[0], _count, (schema_ptr->_field_types->at(f)), dest, omp_get_thread_num(), MPI_COMM_WORLD, &request);
  }

  /**
   * find better hash function
   * @param key
   * @return
   */
  size_t decidePlacement(size_t key) {
    return key%ptr->world;
  }

  void async_send_per_rank(int dest, std::vector<MPI_Request>& requests) {
    CHECK(dest < ptr->world);
    int n = 0;
    for(auto g : *_groups) {
      size_t placement = decidePlacement(g.first);
      if(placement == (size_t)dest) {
        n += g.second.size();
      }
      // LOG(INFO) << placement << " send " << ptr->world;
    }
    int i = 0;
    int array_of_blocklengths[n], displ[n];
    for(auto g : *_groups) {
      size_t placement = decidePlacement(g.first);
      if(placement == (size_t)dest) {
        for(auto item : g.second) {
          array_of_blocklengths[i] = 1;
          displ[i] = (int) item;
          i++;
        }
      }
    }
    CHECK_EQ(i, n);
    if(n == 0) return; //nothing to send to dest

    LOG(INFO) << "send " << n << " to " << dest << "@" << ptr->rank;
    MPI_Datatype keyType;
    MPI_Type_indexed(n, array_of_blocklengths, displ, *schema_ptr->getType(), &keyType);
    MPI_Type_commit(&keyType);
    MPI_Request request;
    MPI_Isend(&_payload[0], 1, keyType, dest,   ptr->stage, MPI_COMM_WORLD, &request);
    requests.push_back(request);
    MPI_Type_free(&keyType);
  }

  /**
   * for dest process, find all aim to send data
   * @param key
   * @param dest
   * @param request
   */
  void async_recv_per_rank(int source, TempTable& out) {
    CHECK(out.schema_ptr->schema_sig() == this->schema_ptr->schema_sig());
    size_t rows = 0;

    for(auto g : *_key_dist) {
      size_t placement = decidePlacement(g.first);
      // LOG(INFO) << placement << " recv " << ptr->world;
      if(placement == (size_t) ptr->rank) {
        for(auto item : g.second) {
          if(item.first == source) {
            rows += item.second;
          }
        }
      }
    }

    if(rows == 0) return; //nothing to recv from source
    LOG(INFO) << "recv " << rows << " from " << source << "@" << ptr->rank;
    // LOG(INFO) << "recv " <<  key << " from " << source << " " << rows << "@" << dest;
    size_t org_size = out._payload.size();
    out._payload.resize(org_size + schema_ptr->size() * rows);
    out._count += rows;
    MPI_Request request;
    MPI_Irecv(&out._payload[org_size], rows, *schema_ptr->getType(), source, ptr->stage, MPI_COMM_WORLD, &request);
    MPI_Status status;
    MPI_Wait(&request, &status);
  }

  /**
   * recv entire column f from source, _payload will be modified
   * @param source
   * @param f
   * @param request
   */
  void async_recv(int source, const Field& f, MPI_Request& request) {
    CHECK_EQ(_pending, 0);
    size_t count;
    MPI_Recv(&count, 1, MPI_UNSIGNED_LONG, source, omp_get_thread_num(), MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    _pending = count;
    if (_pending * (schema_ptr->size()) > _payload.size()) {
      _payload.resize(_pending * (schema_ptr->size()));
    }
    MPI_Irecv(&_payload[0], count, (schema_ptr->_field_types->at(f)), source, omp_get_thread_num(), MPI_COMM_WORLD, &request);
  }
  /**
   * directly read from temptable memory
   * @param index
   * @return
   */
  inline std::unique_ptr<RowBuffer> read(int index) {
    CHECK_LT(index, _count);
    CHECK_NOTNULL(schema_ptr);
    return std::make_unique<RowBuffer>(schema_ptr, &_payload[schema_ptr->size() * index]);
  }

  /**
   * for xgboost, we need to extract list of numerical fields and pass as array of float
   * @param fields features
   * @param data pointer to external float array
   */
  void readFields(std::vector<Field> fields, float* data) {
    CHECK_NOTNULL(data);
    CHECK_GT(fields.size(), 0);

    for (const auto& f : fields) {
      CHECK_EQ(f.type, RowType::DOUBLE);
    }

    size_t columns = fields.size();
    memset(data, 0, sizeof(float) * columns * _count);
    for (size_t i = 0; i < _count; i++) {
      const auto row = read(i);
      for (size_t j = 0; j < columns; j++) {
        Value v;
        row->read(fields[j], v);
        data[i * columns + j] = (float)v.p_val.double_val;
      }
    }
  }

  void readField(const Field& field, float* data) {
    CHECK_NOTNULL(data);
    CHECK_EQ(field.type, RowType::DOUBLE);
    memset(data, 0, sizeof(float) * _count);
    for (size_t i = 0; i < _count; i++) {
      const auto row = read(i);
      Value v;
      row->read(field, v);
      data[i] = (float)v.p_val.double_val;
    }
  }

  void writeField(const Field& field, const float* data) {
    CHECK_NOTNULL(data);
    CHECK_EQ(field.type, RowType::DOUBLE);
    for (size_t i = 0; i < _count; i++) {
      const auto row = read(i);
      Value v;
      v.p_val.double_val = (double)data[i];
      row->write(field, v);
    }
  }

  size_t count() {
    return this->_count;
  }

  void process(XGBOperator& op) {
    std::vector<float> features;
    features.resize(op.features() * _count); // number of features
    readFields(op.fields, &features[0]);     // read from temp table

    std::vector<float> label; // number of labels
    label.resize(_count);     // number of rows

    if (op.parameters.isTraining) {
      readField(op.labelField, &label[0]);
      size_t total_row_count = _count;

      op.gather(&features[0], &label[0], total_row_count, op.features()); //gather training dataset to root
      op.train(&features[0], &label[0], total_row_count, op.features());
      op.syncModel(); // send model to all processes from root
    } else {
      op.predict(&features[0], &label[0], _count, op.features());
      writeField(op.labelField, &label[0]);
    }
    ptr->forward();
  }

  void process(KMeanOperator& op) {
    auto k = op.k;
    auto fields = op.fields;
    double centers[k][fields.size()]; //more efficient ds
    double final_centers[k][fields.size()];

    if (!op.inited) {
      std::unordered_map<int, size_t> local_picks;
      op.init(_count, ptr->rank, ptr->world, local_picks);
      for (auto pick : local_picks) {
        for (size_t i = 0; i < fields.size(); i++) {
          Value v;
          read(pick.second)->read(fields.at(i), v);
          centers[pick.first][i] = v.p_val.double_val;
        }
      }

      MPI_Allreduce(&centers, &final_centers, k * fields.size(), MPI_UNSIGNED_LONG, MPI_MAX, MPI_COMM_WORLD);
      op.inited = true;
      memcpy(op.centers, centers, sizeof(double) * k * fields.size());
    } else {
      memcpy(final_centers, op.centers, sizeof(double) * k * fields.size());
    }
    // step 4, caculate distance and group to k, group to k clusters
    double dist[k]; // each row distance to recvcenter[i]

    for (size_t j = 0; j < _count; j++) {
      for (int point = 0; point < k; point++) {
        auto center = final_centers[point];
        dist[point] = distance(fields, j, center);
      }
      int g = 0; //put on first group
      int shortest = dist[0];
      for (int m = 1; m < k; m++) {
        if (dist[m] < shortest) {
          shortest = dist[m];
          g = m;
        }
      }
      // put row j to group g
      op.addGroup(g, j);
    }
    // step 6, find mean position of each group
    size_t members[k];
    double total[k][fields.size()];
    // group
    for (int i = 0; i < k; i++) {
      members[i] = op.groups.at(i).size();
      // each row in group i
      for (auto index : op.groups.at(i)) {
        // field j in a row index in group i
        for (size_t j = 0; j < fields.size(); j++) {
          Value v;
          read(index)->read(fields.at(j), v);
          total[i][j] += v.p_val.double_val;
        }
      }
    }
    size_t total_members[k];
    MPI_Allreduce(members, total_members, k, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);

    double grandtotal[k][fields.size()];
    MPI_Allreduce(total, grandtotal, k * fields.size(), MPI_DOUBLE, MPI_SUM, MPI_COMM_WORLD);

    // update centers with mean value of each field centers[k][fields.size()];
    for (int i = 0; i < k; i++) {
      for (size_t j = 0; j < fields.size(); j++) {
        centers[i][j] = grandtotal[i][j] / total_members[i];
      }
    }
    memcpy(op.centers, centers, sizeof(double) * k * fields.size());
    op.iteration++;

    if (op.shouldStop()) {
      LOG(INFO) << "complete k-mean training";
      ptr->forward();
    } else {
      //LOG(INFO) << "next iteration of k-mean";
      process(op);
    }
  }

  inline double distance(const std::vector<Field>& fields, size_t index, double center[]) {
    double sum = 0;
    size_t k = 0;
    for (auto f : fields) {
      Value v;
      read(index)->read(f, v);
      sum += std::abs(v.p_val.double_val - center[k]) * std::abs(v.p_val.double_val - center[k]);
      k++;
    }
    return sqrt(sum);
  }

  void group_by(const Field& f) {
    CHECK(schema_ptr->exist(f));
    _groups->clear();
    for (size_t i = 0; i < _count; i++) {
      auto r = read(i);
      Value v;
      r->read(f, v);
      size_t key = value_hasher.operator()(v);
      if (_groups->find(key) == _groups->end()) {
        std::vector<size_t> arr;
        _groups->insert({ key, arr });
      }
      _groups->at(key).push_back(i);
    }
    /**
     * [hash_key, size, rank]
     */
    size_t key_count[_groups->size()][3];
    auto it = _groups->begin();
    int i = 0;
    while (it != _groups->end()) {
      key_count[i][0] = it->first;
      key_count[i][1] = it->second.size();
      key_count[i][2] = ptr->rank;
      i++;
      it++;
    }
    /*
     * merge key_count array into one
     */
    size_t local_group_sizes[ptr->world];
    memset(local_group_sizes, 0, ptr->world * sizeof(size_t)); //always clear memory before use

    int recvcounts[ptr->world], displs[ptr->world];
    memset(recvcounts, 0, ptr->world * sizeof(int));
    memset(displs, 0, ptr->world * sizeof(int));

    displs[0] = 0;
    local_group_sizes[ptr->rank] = _groups->size() * 3;

    size_t recv[ptr->world]; // type should be same as local_group_sizes
    MPI_Allreduce(&local_group_sizes, &recv, ptr->world, MPI_UNSIGNED_LONG, MPI_MAX, MPI_COMM_WORLD);

    std::vector<size_t> _g_groups; // keep key, row counts on each process
    size_t _global_group_size;     // apply to group_by

    _global_group_size = recv[0];
    recvcounts[0] = recv[0];
    for (i = 1; i < ptr->world; i++) {
      displs[i] = displs[i - 1] + recv[i - 1];
      _global_group_size += recv[i];
      recvcounts[i] = (int)recv[i];
    }

    CHECK_GE(_global_group_size, _groups->size() * 3);
    _g_groups.resize(_global_group_size);

    MPI_Allgatherv(key_count, _groups->size() * 3, MPI_UNSIGNED_LONG, &_g_groups[0], recvcounts, displs, MPI_UNSIGNED_LONG, MPI_COMM_WORLD);

    // build key_hash to node and row count map
    for (size_t j = 0; j < _global_group_size; j += 3) {
      size_t key = _g_groups.at(j);
      size_t count = _g_groups.at(j + 1);
      size_t rank = _g_groups.at(j + 2);

      std::pair<int, size_t> pair(rank, count);
      if (_key_dist->find(key) == _key_dist->end()) {
        std::vector<std::pair<int, size_t>> vector;
        vector.push_back(pair);
        _key_dist->insert({ key, vector });
      } else {
        _key_dist->at(key).push_back(pair);
      }
    }
    ptr->forward();
  }

  // group table with
  void group_shuffle(const Field& f1, TempTable& out) {
    CHECK(schema_ptr->exist(f1));
    CHECK_EQ(schema_ptr->schema_sig(), out.schema_ptr->schema_sig());
    //local shuffle key distribution of each table
    this->group_by(f1);
    std::vector<MPI_Request> requests;
    TempTable recv(ptr, schema_ptr);
    recv._payload.resize(0);

    // hash shuffle to world
    for(auto peer = 0 ; peer < ptr->world; peer++) {
      this->async_send_per_rank(peer, requests);
    }

    // read from each rank
    for(auto peer = 0 ; peer < ptr->world; peer++) {
      this->async_recv_per_rank(peer, recv);
    }

    for(auto request : requests) {
      MPI_Status status;
      MPI_Wait(&request, &status);
    }
    this->clear();

    for(size_t s = 0 ; s < recv.count(); s++) {
      Value v;
      auto item = recv.read(s);
      item->read(f1,v);
      out.ingest(*item.get());
    }

    recv.clear();
    ptr->forward();
  }
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_TABLE_H
