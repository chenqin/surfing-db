//
// Created by cq on 2/14/21.
//
#include "processors.h"
#include "mtable.h"
#include "xgbop.h"

namespace surfingdb {
namespace table {

void processors::map(std::shared_ptr<mtable> in, std::shared_ptr<mtable> out, std::function<void(const RowBuffer&, RowBuffer&)> transform) {
  out->reserveRow(in->row_size());
#pragma omp parallel for shared(out) schedule(dynamic, 100)
  for (size_t i = 0; i < in->row_size(); i++) {
    auto in_row = in->readRow(i);
    RowBuffer out_row(out->getSchema());
    transform(*in_row.get(), out_row);
    CHECK_EQ(out_row.schema_sig(), out->getSchema()->signature());
#pragma omp critical(append)
    out->appendRow(out_row);
  }
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
    op.gather(&features_matrix[0], &label_matrix[0], total_row_count, op.features()); //gather training dataset to root
    op.train(&features_matrix[0], &label_matrix[0], total_row_count, op.features());
    op.syncModel(); // send model to all processes from root
  } else {
    op.predict(&features_matrix[0], &label_matrix[0], in->row_size(), op.features());
    in->writeField(op.labelField, &label_matrix[0]);
  }
}

std::shared_ptr<mtable> processors::partition(std::shared_ptr<mtable> in, Field& f) {
  auto schema_ptr = in->getSchema();
  auto node_ptr = in->getNodePtr();
  in->placement_sort(f);
  mtable recv(in->getNodePtr(), in->getSchema(), in->row_size() * in->getSchema()->rowSize());
  MPI_Request revs[node_ptr->world];
  MPI_Request sends[node_ptr->world];
  size_t recv_lens[node_ptr->world];
  for(int i = 0 ; i < node_ptr->world; i++) {
    MPI_Irecv(&recv_lens[i], 1, MPI_UNSIGNED_LONG, i, omp_get_thread_num(), MPI_COMM_WORLD, &revs[i]);
    size_t send_to_i = in->range_row_size(i);
    LOG(INFO) << node_ptr->rank << " => " << i << " size " << send_to_i;
    MPI_Isend(&send_to_i, 1, MPI_UNSIGNED_LONG, i, omp_get_thread_num(), MPI_COMM_WORLD, &sends[i]);
  }

  MPI_Waitall(node_ptr->world, sends, MPI_STATUSES_IGNORE);
  MPI_Waitall(node_ptr->world, revs, MPI_STATUSES_IGNORE);

  for(int i = 0 ; i < node_ptr->world ; i++) {
    LOG(INFO) << node_ptr->rank << " <= " << i << " size " << recv_lens[i];
  }

  size_t buffer_len = 0;
  size_t start_index[node_ptr->world];

  for(int i = 0 ; i < node_ptr->world; i++){
    buffer_len += recv_lens[i];
    start_index[i] = (i == 0) ? 0 : recv_lens[i-1] + start_index[i-1];
  }
  std::vector<uint8_t> buffer;
  buffer.resize(buffer_len * schema_ptr->rowSize());
  int send_count = 0, recv_count =0;

  for(int i = 0 ; i < node_ptr->world; i++) {
    if(recv_lens[i] > 0) {
      CHECK_LE(start_index[i], buffer_len);
      LOG(INFO) << node_ptr->rank << " <- " << i << " size " << recv_lens[i];
      MPI_Irecv(&buffer[start_index[i]*schema_ptr->rowSize()], recv_lens[i], *schema_ptr->schemaMPIType(), i, omp_get_thread_num(), MPI_COMM_WORLD, &revs[recv_count++]);
    }
    if(in->range_row_size(i) > 0) {
      LOG(INFO) << node_ptr->rank << "-> " << i << " size " << in->range_row_size(i);
      MPI_Isend(in->range_ptr(i), in->range_row_size(i), *schema_ptr->schemaMPIType(), i, omp_get_thread_num(), MPI_COMM_WORLD, &sends[send_count++]);
    }
  }
  MPI_Waitall(send_count, sends, MPI_STATUSES_IGNORE);
  MPI_Status statuses[node_ptr->world];
  MPI_Waitall(recv_count, revs, statuses);
  for(int i = 0 ; i < node_ptr->world; i++) {
    if(statuses[i].MPI_ERROR != 0) {
      char buffer[1024];
      int size;
      MPI_Error_string(statuses[i].MPI_ERROR, buffer, &size);
      LOG(INFO) << node_ptr->rank << " from " << i << buffer;
    }
  }
  auto table = std::make_shared<mtable>(node_ptr, schema_ptr, buffer_len * schema_ptr->rowSize());
  memcpy(table->payload_ptr(), &buffer[0], buffer_len * schema_ptr->rowSize());
  table->offset = buffer_len * schema_ptr->rowSize();
  table->row_count = buffer_len;
  buffer.clear();
  buffer.shrink_to_fit();
  LOG(INFO) << "parition complete";
  return table;
}

std::shared_ptr<mtable> processors::partition_rma(std::shared_ptr<mtable> in, Field& f) {
//#pragma omp critical
  in->group(f);
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

  MPI_Win_fence(0, in->win);
  MPI_Datatype type = *(schema_ptr->schemaMPIType());

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