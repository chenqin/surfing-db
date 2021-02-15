//
// Created by cq on 2/14/21.
//
#include "processors.h"
#include "mtable.h"
#include "xgbop.h"

namespace surfingdb {
namespace table {

void processors::map(mtable& in, mtable& out, std::function<void(const RowBuffer&, RowBuffer&)> transform) {
  out.reserveRow(in.row_size());
#pragma omp parallel for shared(out) schedule(dynamic, 100)
  for (size_t i = 0; i < in.row_size(); i++) {
    auto in_row = in.readRow(i);
    RowBuffer out_row(out.getSchema());
    transform(*in_row.get(), out_row);
    CHECK_EQ(out_row.schema_sig(), out.getSchema()->signature());
#pragma omp critical(append)
    out.appendRow(out_row);
  }
}

void processors::xgb(mtable& in, std::vector<Field> features, Field& label, const XGBParameters& parameters) {
  xgbop op(features, label, parameters, in.getNodePtr()->rank, in.getNodePtr()->world);
  std::vector<float> features_matrix;
  features_matrix.resize(op.features() * in.row_size()); // number of features
  in.readFields(op.fields, &features_matrix[0]);         // read from temp table

  std::vector<float> label_matrix;    // number of labels
  label_matrix.resize(in.row_size()); // number of rows

  if (op.parameters.isTraining) {
    in.readField(op.labelField, &label_matrix[0]);
    size_t total_row_count = in.row_size();
#pragma omp critical
    {
      op.gather(&features_matrix[0], &label_matrix[0], total_row_count, op.features()); //gather training dataset to root
      op.train(&features_matrix[0], &label_matrix[0], total_row_count, op.features());
      op.syncModel(); // send model to all processes from root
    }
  } else {
    op.predict(&features_matrix[0], &label_matrix[0], in.row_size(), op.features());
    in.writeField(op.labelField, &label_matrix[0]);
  }
}

void processors::partition(mtable& in, Field& f) {
  auto schema_ptr = in.getSchema();
  auto node_ptr = in.getNodePtr();
  auto row_count = in.row_size();
  MPI_Aint recv_buffer_rows = 0;

  size_t expected_rows[node_ptr->world], expected_start_index[node_ptr->world];
  memset(expected_rows, 0, node_ptr->world * sizeof(size_t));
  memset(expected_start_index, 0, node_ptr->world * sizeof(size_t));

  for (auto k : *in.key_dist) {
    int place = in.placement(k.first);
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
  size_t target_disp[node_ptr->world]; // from each process, tell others start index of assigned memory
  for (int i = 0; i < node_ptr->world; i++) {
    size_t start_index;
    MPI_Scatter(expected_start_index, 1, MPI_UNSIGNED_LONG, &start_index, 1, MPI_UNSIGNED_LONG, i, MPI_COMM_WORLD);
    target_disp[i] = start_index;
  }

  in.reserve_rma_memory(recv_buffer_rows);
  MPI_Win_fence(0, in.win);
  const MPI_Datatype type = *(schema_ptr->schemaMPIType());
  #pragma omp parallel for // exp with 3 hosts shows threading introduce overhead instead of helping
  for (int dest = 0; dest < node_ptr->world; dest++) {
    int ring_dest = (dest + node_ptr->rank) % node_ptr->world;
    uint8_t* rangePtr = in.range_ptr(ring_dest);
    int index = (int)target_disp[ring_dest];
    int count = 0;
    // use adjacent map start index to reason number of rows need to send
    if (ring_dest != node_ptr->world - 1) {
      count = in.placement_index->at(ring_dest + 1) - in.placement_index->at(ring_dest);
    } else {
      count = row_count - in.placement_index->at(ring_dest);
    }
    MPI_Win_lock(MPI_LOCK_SHARED, ring_dest, 0, in.win);
    MPI_Put(rangePtr, count, type, ring_dest, index, count, type, in.win);
    MPI_Win_unlock(ring_dest, in.win);
  }
  MPI_Win_fence(0, in.win);
  // if recv buffer too large, flush to disk and load
  if (recv_buffer_rows > FLUSH_SIZE) {
    in.flush_rma_memory(recv_buffer_rows);
  } else {
    in.copy_rma_memory(recv_buffer_rows);
  }
}
} // namespace table
} // namespace surfingdb