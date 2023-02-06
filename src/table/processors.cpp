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
#include "processors.h"
#include "mtable.h"
#include "xgbop.h"

namespace surfingdb {
namespace table {

std::shared_ptr<mtable> processors::map(std::shared_ptr<mtable> in, std::shared_ptr<TableSchema> out_schema_ptr, std::function<bool(const RowBuffer&, RowBuffer&)> transform) {
  auto out = std::make_shared<mtable>(in->getNodePtr(), out_schema_ptr, in->row_count * out_schema_ptr->rowSize());
  for (size_t i = 0; i < in->row_size(); i++) {
    auto in_row = in->readRow(i);
    RowBuffer out_row(out->getSchema());
    bool append = transform(*in_row.get(), out_row);
    CHECK_EQ(out_row.schema_sig(), out->getSchema()->signature());

    if (append) { out->appendRow(out_row); }
  }
  in->release();
  return out;
}

void processors::reduce(std::shared_ptr<mtable> in_ptr,
                        Field& field,
                        std::shared_ptr<std::unordered_map<Value, std::shared_ptr<RowBuffer>, ValueHasher>> result_ptr,
                        std::shared_ptr<TableSchema> result_schema_ptr,
                        std::function<void(Value&, std::vector<std::unique_ptr<RowBuffer>>&, std::shared_ptr<RowBuffer>&)> reducer) {
  in_ptr->group(field, true);
  for (auto g : *in_ptr->key_groups) {
    auto vals = g.second;
    std::vector<std::unique_ptr<RowBuffer>> val_list;
    Value key;
    for (auto index : vals) {
      auto r = in_ptr->readRow(index);
      r->read(field, key);
      val_list.push_back(std::move(r));
    }
#pragma omp critical
    {
      if (result_ptr->find(key) == result_ptr->end()) {
        result_ptr->insert({ key, std::make_shared<RowBuffer>(result_schema_ptr) });
      }
      std::shared_ptr<RowBuffer> row = result_ptr->at(key);
      reducer(key, val_list, row);
    }
  }
  in_ptr->release();
}

void processors::xgb(std::shared_ptr<mtable> in, std::vector<Field> features, Field& label, const XGBParameters& parameters) {
  xgbop op(features, label, parameters, in->getNodePtr()->rank, in->getNodePtr()->world);
  std::vector<float> features_matrix;
  features_matrix.resize(op.features() * in->row_size()); // number of features
  in->readFields(op.fields, &features_matrix[0]);         // read from temp table

  std::vector<float> label_matrix;     // number of labels
  label_matrix.resize(in->row_size()); // number of rows

  if (op.parameters.isTraining) {
    in->readField(op.labelField, &label_matrix[0]);
    size_t total_row_count = in->row_size();
    op.gather(&features_matrix[0], &label_matrix[0], total_row_count, op.features()); // gather training dataset to root
    op.train(&features_matrix[0], &label_matrix[0], total_row_count, op.features());
    op.syncModel(); // send model to all processes from root
  } else {
    op.predict(&features_matrix[0], &label_matrix[0], in->row_size(), op.features());
    in->writeField(op.labelField, &label_matrix[0]);
  }
}

std::shared_ptr<mtable> processors::shuffle(std::shared_ptr<mtable> in, Field& f, bool all) {
  auto schema_ptr = in->getSchema();
  auto node_ptr = in->getNodePtr();
  /**
   * verify all workers in same stage
   */
  node_ptr->forward();

  /**
   * register and commit schema row size unit to all workers
   */
  MPI_Datatype row_type;
  MPI_Type_contiguous(schema_ptr->rowSize(), MPI_CHAR, &row_type);
  MPI_Type_commit(&row_type);

  mtable recv(in->getNodePtr(), in->getSchema(), in->row_size() * in->getSchema()->rowSize());
  MPI_Request sends[node_ptr->world];
  MPI_Request recvs[node_ptr->world];
  size_t tranfer_row_counts[node_ptr->world];
  if (all) {
    /**
     * send_ct_per_rank - number of rows will be send from index rank
     * rev_ct_per_rank - number of rows will be recieved from index rank
     */
    std::vector<int> send_ct_per_rank(node_ptr->world), rev_ct_per_rank(node_ptr->world);
    for (int i = 0; i < node_ptr->world; i++) {
      send_ct_per_rank.push_back(in->range_row_size(i));
    }
    MPI_Alltoall(send_ct_per_rank.data(), 1, MPI_INT, rev_ct_per_rank.data(), 1, MPI_INT, MPI_COMM_WORLD);
    CHECK(send_ct_per_rank.at(node_ptr->rank) == 0);
    CHECK(rev_ct_per_rank.at(node_ptr->rank) == 0);

    /**
     * calculate displment of send and recv
     */
    std::vector<int> send_ct_disp(node_ptr->world);
    std::partial_sum(send_ct_per_rank.begin(), send_ct_per_rank.end(),
                     send_ct_disp.begin());
    std::vector<int> send_ct_dispment(node_ptr->world);
    send_ct_dispment[0] = 0;
    std::copy(send_ct_disp.begin(), send_ct_disp.end() - 1,
              send_ct_dispment.begin() + 1);

    std::vector<int> recv_ct_disp(node_ptr->world);
    std::partial_sum(rev_ct_per_rank.begin(), rev_ct_per_rank.end(),
                     recv_ct_disp.begin());
    std::vector<int> recv_ct_dispment(node_ptr->world);
    recv_ct_dispment[0] = 0;
    std::copy(recv_ct_disp.begin(), recv_ct_disp.end() - 1,
              recv_ct_dispment.begin() + 1);

    int recv_size = 0;
    recv_size = std::accumulate(rev_ct_per_rank.begin(), rev_ct_per_rank.end(), recv_size);

    std::vector<uint8_t> recv_buffer;
    recv_buffer.resize(recv_size * schema_ptr->rowSize());
    MPI_Request request;
    MPI_Status status;
    MPI_Ialltoallv(in->payload_ptr(), &send_ct_per_rank[0], &send_ct_dispment[0], row_type, &recv_buffer[0], &rev_ct_per_rank[0], &recv_ct_dispment[0], row_type, MPI_COMM_WORLD, &request);
    MPI_Wait(&request, &status);
    in->release();

    /**
     * copy buffer to new table
     */
    auto table = std::make_shared<mtable>(node_ptr, schema_ptr, recv_size * schema_ptr->rowSize());
    memcpy(table->payload_ptr(), &recv_buffer[0], recv_size * schema_ptr->rowSize());
    table->offset = recv_size * schema_ptr->rowSize();
    table->row_count = recv_size;
    recv_buffer.clear();
    recv_buffer.shrink_to_fit();
    MPI_Type_free(&row_type);
    return table;
  } else {
    for (int j = 0; j < node_ptr->world; j++) {
      int i = (j + (omp_get_thread_num() << 1)) % node_ptr->world;
      if (node_ptr->rank == i) {
        size_t send_to_i = in->range_row_size(i);
        MPI_Request request;
        MPI_Status status;
        int tag = node_ptr->rank * node_ptr->world + i;
        MPI_Isend(&send_to_i, 1, MPI_UNSIGNED_LONG, i, tag, MPI_COMM_WORLD, &request);
        for (int j = 0; j < node_ptr->world; j++) {
          tag = j * node_ptr->world + i;
          MPI_Recv(&tranfer_row_counts[j], 1, MPI_UNSIGNED_LONG, j, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        }
        MPI_Wait(&request, &status);
      } else {
        int tag = node_ptr->rank * node_ptr->world + i;
        size_t send_to_i = in->range_row_size(i);
        MPI_Send(&send_to_i, 1, MPI_UNSIGNED_LONG, i, tag, MPI_COMM_WORLD);
      }
    }

    size_t recv_row_count = 0;
    size_t transfered_row_index_rank[node_ptr->world];

    for (int i = 0; i < node_ptr->world; i++) {
      recv_row_count += tranfer_row_counts[i];
      transfered_row_index_rank[i] = (i == 0) ? 0 : tranfer_row_counts[i - 1] + transfered_row_index_rank[i - 1];
    }

    MPI_Barrier(MPI_COMM_WORLD);
    auto table = std::make_shared<mtable>(node_ptr, schema_ptr, recv_row_count * schema_ptr->rowSize());
    int mpi_buffer_size = (int)(recv_row_count * schema_ptr->rowSize());

    CHECK(mpi_buffer_size < MEM_PAGE_SIZE); // no more than given table size

    int send_count = 0, recv_count = 0;

    for (int i = 0; i < node_ptr->world; i++) {
      int send_rank_offset = i;
      if (in->range_row_size(send_rank_offset) > 0) {
        // LOG(INFO) << node_ptr->rank << "-> " << i << " size " << in->range_row_size(i);
        /**
         * @brief tag is unqiue value from sender to reciever with two dim array
         *
         */
        int tag = node_ptr->rank * (node_ptr->world) + send_rank_offset;
        MPI_Isend(in->range_ptr(send_rank_offset), in->range_row_size(send_rank_offset), row_type, send_rank_offset, tag, MPI_COMM_WORLD, &sends[send_count++]);
      }
      int recv_rank_offset = i;
      if (tranfer_row_counts[recv_rank_offset] > 0) {
        CHECK_LE(transfered_row_index_rank[recv_rank_offset], recv_row_count);
        // LOG(INFO) << node_ptr->rank << " <- " << i << " size " << recv_lens[i];
        /**
         * @brief tag is unqiue value from sender to reciever with two dim array
         * matching sender side
         */
        int tag = recv_rank_offset * (node_ptr->world) + node_ptr->rank;
        MPI_Irecv(&table->payload[transfered_row_index_rank[recv_rank_offset] * schema_ptr->rowSize()], tranfer_row_counts[recv_rank_offset], row_type, recv_rank_offset, tag, MPI_COMM_WORLD, &recvs[recv_count++]);
      }
    }
    MPI_Status statuses[node_ptr->world];
    MPI_Waitall(send_count, sends, statuses);
    MPI_Waitall(recv_count, recvs, statuses);
    table->offset = recv_row_count * schema_ptr->rowSize();
    table->row_count = recv_row_count;

    in->release();
    MPI_Type_free(&row_type);
    return table;
  }
}

std::shared_ptr<mtable> processors::shuffleRMA(std::shared_ptr<mtable> in, Field& f) {
  // #pragma omp critical
  in->group(f, false);
  in->placement_sort(f);
  CHECK(!in->placement_index->empty());
  CHECK_NOTNULL(in->getSchema());

  auto schema_ptr = in->getSchema();
  auto node_ptr = in->getNodePtr();
  auto row_count = in->row_size();
  MPI_Aint recv_buffer_rows = 0;

  size_t expected_rows[node_ptr->world], expected_start_index[node_ptr->world];
  memset(expected_rows, 0, node_ptr->world * sizeof(size_t));
  memset(expected_start_index, 0, node_ptr->world * sizeof(size_t));

  for (auto k : *in->key_dist) {
    int place = in->placement(k.first);
    if (place == node_ptr->rank) {
      for (auto p : k.second) {
        recv_buffer_rows += p.second;
        expected_rows[p.first] += p.second;
      }
    }
  }

  for (int i = 1; i < node_ptr->world; i++) {
    expected_start_index[i] = expected_start_index[i - 1] + expected_rows[i - 1];
  }

  // LOG(INFO) << "start exchange index " << node_ptr->rank;
  MPI_Request requests[node_ptr->world];
  size_t target_disp[node_ptr->world]; // from each process, tell others start index of assigned memory
  for (int i = 0; i < node_ptr->world; i++) {
    MPI_Request request;
    MPI_Iscatter(expected_start_index, 1, MPI_UNSIGNED_LONG, &target_disp[i], 1, MPI_UNSIGNED_LONG, i, MPI_COMM_WORLD, &request);
    requests[i] = request;
  }
  in->reserve_rma_memory(recv_buffer_rows);

  MPI_Datatype type;
  MPI_Type_contiguous(schema_ptr->rowSize(), MPI_CHAR, &type);
  MPI_Type_commit(&type);

  MPI_Win_fence(0, in->win);
  for (int dest = 0; dest < node_ptr->world; dest++) {
    int ring_dest = (dest + node_ptr->rank) % node_ptr->world;
    uint8_t* rangePtr = in->range_ptr(ring_dest);
    // lazy evaluate scatter result
    MPI_Wait(&requests[ring_dest], MPI_STATUS_IGNORE);
    int index = (int)target_disp[ring_dest];
    int count = 0;
    // use adjacent map start index to reason number of rows need to send
    if (ring_dest != node_ptr->world - 1) {
      count = in->placement_index->at(ring_dest + 1) - in->placement_index->at(ring_dest);
    } else {
      count = row_count - in->placement_index->at(ring_dest);
    }
    MPI_Win_lock(MPI_LOCK_SHARED, ring_dest, 0, in->win);
    MPI_Put(rangePtr, count, type, ring_dest, index, count, type, in->win);
    MPI_Win_unlock(ring_dest, in->win);
  }
  MPI_Win_fence(0, in->win);

  MPI_Type_free(&type);
  // if recv buffer too large, flush to disk and load
  if (recv_buffer_rows > FLUSH_SIZE) {
    in->flush_rma_memory(recv_buffer_rows);
  } else {
    in->copy_rma_memory(recv_buffer_rows);
  }
  return in;
}

} // namespace table
} // namespace surfingdb