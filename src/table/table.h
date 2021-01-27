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

  Field _parition;                                                                            //partition field
  std::unique_ptr<std::unordered_map<size_t, std::vector<std::pair<int, size_t>>>> _key_dist; // key hash and per node counts
  std::unique_ptr<std::unordered_map<size_t, std::vector<size_t>>> _groups;                   // local key, offsets map

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
    _key_dist = std::make_unique<std::unordered_map<size_t, std::vector<std::pair<int, size_t>>>>();
    _groups = std::make_unique<std::unordered_map<size_t, std::vector<size_t>>>();
    LOG(INFO) << "for testing only";
  }
  TempTable(const std::shared_ptr<Node> node, const std::shared_ptr<TableSchema> sharedPtr) {
    this->schema_ptr = sharedPtr;
    _payload.resize(MEM_PAGE_SIZE);
    offset = 0;
    _count = 0;
    _key_dist = std::make_unique<std::unordered_map<size_t, std::vector<std::pair<int, size_t>>>>();
    _groups = std::make_unique<std::unordered_map<size_t, std::vector<size_t>>>();
    this->ptr = node;
  }

  ~TempTable() {
    _payload.clear();
    offset = 0;
    _count = 0;
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
    LOG(INFO) << "flushed to " << path;
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
    _parition = f;
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

  inline void gather(std::vector<std::pair<int, size_t>>& left, size_t key, int r, TempTable& global_mtable) {
    auto pruned_schema = global_mtable.schema_ptr;
    auto pruned_type = pruned_schema->getType();
    CHECK_NOTNULL(pruned_schema.get());
    // gatherv rows of key to r
    int recvcounts[ptr->world], displs[ptr->world];
    memset(recvcounts, 0, ptr->world * sizeof(int));
    memset(displs, 0, ptr->world * sizeof(int));
    displs[0] = 0;
    size_t m_size = 0;

    for (int i = 0; i < ptr->world; i++) {
      for (auto s : left) {
        if (s.first == i) {
          recvcounts[i] = s.second;
          m_size += s.second;
        } else {
          recvcounts[i] = 0;
        }
        if (i > 0) {
          displs[i] = displs[i - 1] + recvcounts[i - 1];
        }
      }
    }
    TempTable mtable(global_mtable.ptr, pruned_schema); // after column pruning

    if (this->_groups.get()->find(key) == this->_groups.get()->end()) {
      mtable._payload.resize(0);
    } else {
      std::vector<size_t> offsets = this->_groups->at(key);
      for (size_t it : offsets) {
        RowBuffer out(pruned_schema);
        auto rorg = read(it);
        for (auto f : pruned_schema.get()->fields) {
          Value v;
          rorg->read(f, v);
          out.write(f, v);
        }
        mtable.ingest(out);
      }
    }
    global_mtable._count = m_size;
    global_mtable._payload.resize(pruned_schema->size() * m_size);
    MPI_Gatherv(mtable.payload_ptr(), mtable._count, *pruned_type, global_mtable.payload_ptr(), recvcounts, displs, *pruned_type, r, MPI_COMM_WORLD);
  }

  // group table with
  void join(const Field& f1, const Field& f2, TempTable& table2, TempTable& out) {
    CHECK(schema_ptr->exist(f1));
    CHECK(table2.schema_ptr->exist(f2));
    CHECK(f1.type == f2.type);                                     //key should be same type
    CHECK(out.schema_ptr->exist(f1) || out.schema_ptr->exist(f2)); //force output has key columns
    //local shuffle key distribution of each table
    this->group_by(f1);
    table2.group_by(f2);

    int r = 0;
    std::vector<size_t> union_keys; // find list of keys where join matches
    // union key_distributions
    for (auto pair : *_key_dist.get()) {
      if (table2._key_dist->find(pair.first) != table2._key_dist->end()) {
        // inner joined set
        union_keys.push_back(pair.first);
      }
    }

    LOG(INFO) << "TOTAL KEYS " << union_keys.size();
    // physical gather join, r should be where rows aggregated
    for (const auto s : union_keys) {
      std::vector<std::pair<int, size_t>> left = _key_dist->at(s);
      std::vector<std::pair<int, size_t>> right = table2._key_dist->at(s);

      RowSchema ls, rs; //prune list of columns used in out table
      for (auto of : out.schema_ptr->fields) {
        if (schema_ptr->exist(of)) {
          ls.fields.push_back(of);
        }
        // todo if field has same name and type
        if (table2.schema_ptr->exist(of)) {
          rs.fields.push_back(of);
        } else {
          CHECK(false);
        }
      }

      TempTable global_mtable(this->ptr, std::make_shared<TableSchema>(ls));
      TempTable global_ntable(table2.ptr, std::make_shared<TableSchema>(rs));
      gather(left, s, r, global_mtable); // collect selected columns of each table
      gather(right, s, r, global_ntable);

      // permutation of m elements with n elements
      if (ptr->rank == r) {
        for (size_t mi = 0; mi < global_mtable._count; mi++) {
          auto m = global_mtable.read(mi);
          for (size_t ni = 0; ni < global_ntable._count; ni++) {
            auto n = global_ntable.read(ni);

            RowBuffer row(out.schema_ptr);
            // extract field from m , n respectively
            for (auto f : out.schema_ptr->fields) {
              // check m fields
              for (auto mf : ls.fields) {
                if (f == mf) {
                  Value v;
                  m->read(mf, v);
                  row.write(f, v);
                  break;
                }
              }
              // check n fields
              for (auto nf : rs.fields) {
                if (f == nf) {
                  Value v;
                  n->read(nf, v);
                  row.write(f, v);
                  break;
                }
              }
            }
            out.ingest(row); //write permutated result to out
          }
        }
      }
      // do mxn rows in out
      r = (r + 1) % ptr->world;
    }

    // unlike gather model where all rows are physically aggregated to same processes sequentially
    // TODO(chenqin): shuffle (broadcast, hash) could write to disk buffer with dest |key1| rows|key2|rows| and run async send
    // since _key_dist is shared in all process, each communication could be reasoned independently hence reciver could starts async recv as well
    ptr->forward();
  }
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_TABLE_H
