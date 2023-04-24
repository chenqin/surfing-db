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
#include <arrow/c/bridge.h>
#include "mtable.h"
#include "utils.h"
#include "xgbop.h"

namespace surfingdb {
namespace table {

/**
 * @brief scan through each row, apply transform function, append to output table if return true
 *
 * @param in  input micro batch table
 * @param out_schema_ptr  output micro batch table schema
 * @param transform transform function with simple type
 * @return std::shared_ptr<mtable>  outputtable
 */
std::shared_ptr<mtable> processors::map(std::shared_ptr<mtable> input, std::shared_ptr<mschema> out_schema_ptr, std::function<bool(mrow&, mrow&, const mschema&)> transform) {
  auto out_table_ptr = std::make_shared<mtable>(input->getNodePtr(), out_schema_ptr, input->row_count * out_schema_ptr->rowSize());
  for (size_t i = 0; i < input->row_count; i++) {
    auto in_row = input->readRow(i);
    /**
     * @brief use out table memory to avoid memcpy
     */
    mrow shaddlow_out_row(out_table_ptr->getSchema(), out_table_ptr->payload_ptr() + out_table_ptr->offset);
    bool append = transform(*in_row.get(), shaddlow_out_row, *out_schema_ptr.get());
    CHECK_EQ(shaddlow_out_row.schema_sig(), out_schema_ptr->signature());

    /**
     * @brief update offset and row_count
     *
     */
    if (append) {
      out_table_ptr->row_count++;
      out_table_ptr->offset += out_table_ptr->getSchema()->rowSize();
      CHECK_LE(out_table_ptr->row_count, input->row_count);
      CHECK_LE(out_table_ptr->offset, input->row_count * out_schema_ptr->rowSize());
    } else {
      /**
       * @brief reset memory
       *
       */
      memset(shaddlow_out_row.payload_ptr(), 0, out_table_ptr->getSchema()->rowSize());
    }
  }
  return out_table_ptr;
}

void processors::reduce(std::shared_ptr<mtable> in_ptr,
                        Field& field,
                        std::shared_ptr<std::unordered_map<Value, std::shared_ptr<mrow>, ValueHasher>> result_ptr,
                        std::shared_ptr<mschema> result_schema_ptr,
                        std::function<void(Value&, std::vector<std::unique_ptr<mrow>>&, std::shared_ptr<mrow>&)> reducer) {
  in_ptr->group(field, true);
  for (auto g : *in_ptr->key_groups) {
    auto vals = g.second;
    std::vector<std::unique_ptr<mrow>> val_list;
    Value key;
    for (auto index : vals) {
      auto r = in_ptr->readRow(index);
      r->read(field, key);
      val_list.push_back(std::move(r));
    }
    {
      if (result_ptr->find(key) == result_ptr->end()) {
        result_ptr->insert({ key, std::make_shared<mrow>(result_schema_ptr) });
      }
      std::shared_ptr<mrow> row = result_ptr->at(key);
      reducer(key, val_list, row);
    }
  }
}

void processors::xgb(std::shared_ptr<mtable> input, std::vector<Field> features, Field& label, const XGBParameters& parameters) {
  xgbop op(features, label, parameters, input->getNodePtr()->rank, input->getNodePtr()->world);
  std::vector<float> features_matrix;
  features_matrix.resize(op.features() * input->row_size()); // number of features
  input->readFields(op.fields, &features_matrix[0]);         // read from temp table

  std::vector<float> label_matrix;                           // number of labels
  label_matrix.resize(input->row_size());                    // number of rows

  if (op.parameters.isTraining) {
    input->readField(op.labelField, &label_matrix[0]);
    size_t total_row_count = input->row_size();
    op.gather(&features_matrix[0], &label_matrix[0], total_row_count, op.features()); // gather training dataset to root
    op.train(&features_matrix[0], &label_matrix[0], total_row_count, op.features());
    op.syncModel();                                                                   // send model to all processes from root
  } else {
    op.predict(&features_matrix[0], &label_matrix[0], input->row_size(), op.features());
    input->writeField(op.labelField, &label_matrix[0]);
  }
}

std::shared_ptr<mtable> processors::shuffle_one_side(std::shared_ptr<mtable> input, Field& f, std::function<size_t(size_t key, int rank, int world)> partitioner) {

  CHECK(input->sorted);

  auto schema_ptr = input->getSchema();
  auto node_ptr = input->getNodePtr();
  int rank = node_ptr->rank;
  int world = node_ptr->world;
  size_t rowsize = schema_ptr->rowSize();
  /**
   * register and commit schema row size unit to all workers
   */
  MPI_Datatype row_type;
  MPI_Type_contiguous(rowsize, MPI_CHAR, &row_type);
  MPI_Type_commit(&row_type);

  size_t send_to_vec[world], recv_from_vec[world];

  for (int i = 0; i < world; i++) {
    send_to_vec[i] = input->range_row_size(i);
  }
  /**
   * @brief recv_from_vec stores number of rows current rank expect to get from peers
   * send_to_vec stores number of rows current rank expect to send to other peers
   *
   */
  MPI_Alltoall(&send_to_vec, 1, MPI_UNSIGNED_LONG, recv_from_vec, 1, MPI_UNSIGNED_LONG, MPI_COMM_WORLD);

  size_t recv_bytes = 0;
  size_t send_row_index_rank[world], get_row_index_rank[world];
  size_t recv_row_offset[world];

  // save start offset index send to each rank i
  for (int i = 0; i < world; i++) {
    // std::cout << rank << " <->" << i << " <" << send_to_vec[i] << ", " << recv_from_vec[i] << ">"<< std::endl;
    recv_bytes += recv_from_vec[i];
    send_row_index_rank[i] = (i == 0) ? 0 : send_to_vec[i - 1] + send_row_index_rank[i - 1];
    recv_row_offset[i] = (i == 0) ? 0 : recv_from_vec[i - 1] + recv_row_offset[i - 1];
  }
  /**
   * @brief send_row_index_rank stores start index current rank send to peer
   * g_row_index_rank stores peer start index current rank expect get data from
   *
   */
  MPI_Alltoall(&send_row_index_rank, 1, MPI_UNSIGNED_LONG, get_row_index_rank, 1, MPI_UNSIGNED_LONG, MPI_COMM_WORLD);
  auto table = std::make_shared<mtable>(node_ptr, schema_ptr, recv_bytes * rowsize);
  // copy local rows from input to new table
  if (get_row_index_rank[rank] > 0) {
    void* dest_ptr = table->payload_ptr() + rowsize * get_row_index_rank[rank];
    memcpy(dest_ptr, input->range_ptr(rank), send_to_vec[rank] * rowsize);
  }

  MPI_Request gets[node_ptr->world];
  MPI_Status statuses[node_ptr->world];

  input->build_window();
  MPI_Win_fence(0, input->win);
  int toal_requests = 0;
  for (int dest = 0; dest < world; dest++) {
    // mpi put to local rank will silently fail, if no data fetching also skip MPI_GET
    if (dest == rank || recv_from_vec[dest] == 0) continue;
    MPI_Aint offset = get_row_index_rank[dest];
    CHECK_EQ(MPI_SUCCESS,
             MPI_Rget(table->payload_ptr() + rowsize * recv_row_offset[dest], recv_from_vec[dest], row_type, dest, offset, recv_from_vec[dest], row_type, input->win, &gets[toal_requests++]));
  }
  MPI_Waitall(toal_requests, gets, statuses);

  MPI_Win_fence(0, input->win);
  input->release_window();
  MPI_Type_free(&row_type);

  table->offset = recv_bytes * rowsize;
  table->row_count = recv_bytes;
  return table;
}

static void release_malloced_type(struct ArrowSchema* schema) {
  if (schema->release == NULL) return;
  int i;
  for (i = 0; i < schema->n_children; ++i) {
    struct ArrowSchema* child = schema->children[i];
    if (child->release != NULL) {
      child->release(child);
    }
  }
  free(schema->children);
  // Mark released
  schema->release = NULL;
}

static void release_malloced_array(struct ArrowArray* array) {
  if (array->release == NULL) return;
  int i;
  // Free children
  for (i = 0; i < array->n_children; ++i) {
    struct ArrowArray* child = array->children[i];
    if (child->release != NULL) {
      child->release(child);
    }
  }
  free(array->children);
  // Free buffers
  for (i = 0; i < array->n_buffers; ++i) {
    free((void*)array->buffers[i]);
  }
  free(array->buffers);
  // Mark released
  array->release = NULL;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> processors::shuffle_two_side(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, std::string filedname, std::function<size_t(size_t, int, int)> partitioner, int rank, int world) {
  MPI_Request sends[world];
  MPI_Request recvs[world];
  MPI_Status statuses[world];

  size_t send_to_vec[world], recv_from_vec[world];
  std::vector<std::shared_ptr<arrow::Buffer>> send_buffers(world);

  int total_batches = 0;
  int local_batches = batches.size();
  MPI_Allreduce(&local_batches, &total_batches, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);

  // nothing to shuffle
  if (total_batches == 0) {
    return batches;
  }

  size_t total_rows = 0;
  for (auto& batch : batches) {
    total_rows += batch->num_rows();
  }
  size_t orgin_row_sum = 0;
  MPI_Allreduce(&total_rows, &orgin_row_sum, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);

  CHECK_GT(local_batches, 0);

  auto schema = batches[0]->schema();

  for (int j = 0; j < world; j++) {
    auto send_to_dest_table = utils::group(batches, filedname, partitioner, j, rank, world);
    auto buffer = utils::serialize(send_to_dest_table);
    send_buffers[j] = buffer;
    send_to_vec[j] = buffer->size();
  }

  MPI_Alltoall(&send_to_vec, 1, MPI_UNSIGNED_LONG, recv_from_vec, 1, MPI_UNSIGNED_LONG, MPI_COMM_WORLD);

  size_t recv_bytes = 0;
  size_t tranfer_bytes_rank_index[world];
  for (int i = 0; i < world; i++) {
    recv_bytes += recv_from_vec[i];
    tranfer_bytes_rank_index[i] = (i == 0) ? 0 : recv_from_vec[i - 1] + tranfer_bytes_rank_index[i - 1];
  }
  auto maybe_buffer = arrow::AllocateBuffer(recv_bytes);
  CHECK(maybe_buffer.ok());
  std::shared_ptr<arrow::Buffer> recv_buffer = std::move(maybe_buffer.ValueOrDie());
  int send_count = 0, recv_count = 0;

  for (int i = 0; i < world; i++) {
    int send_to_rank = i;
    if (send_to_vec[send_to_rank] > 0) {
      int tag = rank * world + send_to_rank;
      MPI_Isend(send_buffers[send_to_rank]->data(), send_to_vec[send_to_rank], MPI_CHAR, send_to_rank, tag, MPI_COMM_WORLD, &sends[send_count++]);
    }
    int recv_from_rank = i;
    if (recv_from_vec[recv_from_rank] > 0) {
      CHECK_LE(tranfer_bytes_rank_index[recv_from_rank], recv_bytes);
      int tag = recv_from_rank * world + rank;
      MPI_Irecv(recv_buffer->mutable_data() + tranfer_bytes_rank_index[recv_from_rank], recv_from_vec[recv_from_rank], MPI_CHAR, recv_from_rank, tag, MPI_COMM_WORLD, &recvs[recv_count++]);
    }
  }
  MPI_Waitall(send_count, sends, statuses);
  MPI_Waitall(recv_count, recvs, statuses);

  std::vector<std::shared_ptr<arrow::RecordBatch>> arrow_tables;
  for (int i = 0; i < world; i++) {
    if (recv_from_vec[i] > 0) {
      auto _buffer = arrow::SliceBuffer(recv_buffer, tranfer_bytes_rank_index[i], recv_from_vec[i]);
      auto arrow_table = utils::deserialize(_buffer, schema);
      arrow_tables.push_back(arrow_table);
    }
  }
  total_rows = 0;
  for (auto& batch : arrow_tables) {
    total_rows += batch->num_rows();
  }

  size_t shuffle_row_sum = 0;
  MPI_Allreduce(&total_rows, &shuffle_row_sum, 1, MPI_UNSIGNED_LONG, MPI_SUM, MPI_COMM_WORLD);
  // check before and after shuffle total row count equal
  CHECK_EQ(shuffle_row_sum, orgin_row_sum);
  
  return arrow_tables;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> processors::shuffle_x(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batch, std::string field_name, std::function<size_t(size_t, int, int)> partitioner, bool singleside, int rank, int world) {
  auto start = MPI_Wtime();
  auto out = processors::shuffle_two_side(batch, field_name, partitioner, rank, world);
  LOG(INFO) << "shuffle takes " << MPI_Wtime() - start << " seconds";
  // TODO: we observed a bug in tcp MPI_GET that lead to one row of garbage
  return out;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> processors::java_x(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batch, std::string class_name, const std::shared_ptr<node>& node) {
  std::vector<std::shared_ptr<arrow::RecordBatch>> out;
  for (auto& b : batch) {
    auto result = java(b, class_name, node);
    out.push_back(std::move(result));
  }
  return std::move(out);
}

std::shared_ptr<arrow::RecordBatch> processors::java(const std::shared_ptr<arrow::RecordBatch>& batch, std::string class_name, const std::shared_ptr<node>& node) {
  const jclass bridge = node->env->FindClass(class_name.c_str());
  CHECK_NOTNULL(bridge);
  struct ArrowSchema arrowSchemaIn, arrowSchemaOut;
  struct ArrowArray arrowArrayIn, arrowArrayOut;
  const jmethodID invoke_method = node->env->GetStaticMethodID(bridge, std::string(BRIDGE_METHOD_NAME).c_str(), "(JJJJ)V");
  CHECK_NOTNULL(invoke_method);

  /**
   * @brief export schema and data
   *
   */
  auto schema_ptr = batch->schema();
  arrow::ExportSchema(*schema_ptr.get(), &arrowSchemaIn);
  arrow::ExportRecordBatch(*batch.get(), &arrowArrayIn, &arrowSchemaIn);
  /**
   * @brief invoke java method, passing pointers
   *
   */
  node->env->CallStaticVoidMethod(bridge, invoke_method,
                                  static_cast<jlong>(reinterpret_cast<uintptr_t>(&arrowSchemaIn)),
                                  static_cast<jlong>(reinterpret_cast<uintptr_t>(&arrowArrayIn)),
                                  static_cast<jlong>(reinterpret_cast<uintptr_t>(&arrowSchemaOut)),
                                  static_cast<jlong>(reinterpret_cast<uintptr_t>(&arrowArrayOut)));

  if (node->env->ExceptionCheck()) {
    LOG(ERROR) << "fail to call jni";
    node->env->DeleteLocalRef(bridge);
    release_malloced_array(&arrowArrayIn);
    release_malloced_array(&arrowArrayOut);
    release_malloced_type(&arrowSchemaIn);
    release_malloced_type(&arrowSchemaOut);
    return java(batch, class_name, node);
  }
  node->env->DeleteLocalRef(bridge);
  /**
   * @brief import schema and data from java
   *
   */
  const auto resultImportVectorSchemaRoot = arrow::ImportRecordBatch(&arrowArrayOut, &arrowSchemaOut);
  std::shared_ptr<arrow::RecordBatch> recordBatch = resultImportVectorSchemaRoot.ValueOrDie();
  release_malloced_array(&arrowArrayIn);
  release_malloced_array(&arrowArrayOut);
  release_malloced_type(&arrowSchemaIn);
  release_malloced_type(&arrowSchemaOut);
  return recordBatch;
}

} // namespace table
} // namespace surfingdb