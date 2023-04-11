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
std::shared_ptr<mtable> processors::map(std::shared_ptr<mtable> in, std::shared_ptr<mschema> out_schema_ptr, std::function<bool(mrow&, mrow&, const mschema&)> transform) {
  auto out_table_ptr = std::make_shared<mtable>(in->getNodePtr(), out_schema_ptr, in->row_count * out_schema_ptr->rowSize());
  for (size_t i = 0; i < in->row_count; i++) {
    auto in_row = in->readRow(i);
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
      CHECK_LE(out_table_ptr->row_count, in->row_count);
      CHECK_LE(out_table_ptr->offset, in->row_count * out_schema_ptr->rowSize());
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

std::shared_ptr<mtable> processors::shuffle(std::shared_ptr<mtable> input, Field& f, std::function<size_t(size_t key, int rank, int world)> partitioner, bool sorted) {
  auto schema_ptr = input->getSchema();
  auto node_ptr = input->getNodePtr();
  int rank = node_ptr->rank;
  int world = node_ptr->world;
  size_t rowsize = schema_ptr->rowSize();

  /**
   * @brief sort input rows if not
   *
   */
  auto in = input->placement_sort(f, partitioner);
  /**
   * register and commit schema row size unit to all workers
   */
  MPI_Datatype row_type;
  MPI_Type_contiguous(rowsize, MPI_CHAR, &row_type);
  MPI_Type_commit(&row_type);

  MPI_Request sends[world];
  MPI_Request recvs[world];
  MPI_Status statuses[world];

  size_t send_to_vec[world], recv_from_vec[world];

  for (int j = 0; j < world; j++) {
    send_to_vec[j] = in->range_row_size(j);
  }
  /**
   * @brief recv_from_vec stores number of rows current rank expect to get from peers
   * send_to_vec stores number of rows current rank expect to send to other peers
   * 
   */
  MPI_Alltoall(&send_to_vec, 1, MPI_UNSIGNED_LONG, recv_from_vec, 1, MPI_UNSIGNED_LONG, MPI_COMM_WORLD);

  size_t recv_row_count = 0;
  size_t send_row_index_rank[world], get_row_index_rank[world];

  // save start offset index send to each rank i
  for (int i = 0; i < world; i++) {
    // std::cout << rank << " <->" << i << " <" << send_to_vec[i] << ", " << recv_from_vec[i] << ">"<< std::endl;
    recv_row_count += recv_from_vec[i];
    send_row_index_rank[i] = (i == 0) ? 0 : send_to_vec[i - 1] + send_row_index_rank[i - 1];
  }
  /**
   * @brief send_row_index_rank stores start index current rank send to peer
   * g_row_index_rank stores peer start index current rank expect get data from
   * 
   */
  MPI_Alltoall(&send_row_index_rank, 1, MPI_UNSIGNED_LONG, get_row_index_rank, 1, MPI_UNSIGNED_LONG, MPI_COMM_WORLD);
  auto table = std::make_shared<mtable>(node_ptr, schema_ptr, recv_row_count * rowsize);

  input->build_window();
  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Win_fence(0, input->win);
  for (int dest = 0; dest < world; dest++) {
    // mpi put to local rank will silently fail
    //if (dest == rank) continue;
    MPI_Aint offset = get_row_index_rank[dest];
    CHECK_EQ(MPI_SUCCESS,
             MPI_Get(table->payload_ptr() + rowsize * table->offset, recv_from_vec[dest], row_type, dest, offset, recv_from_vec[dest], row_type, input->win));
    std::cout << rank << " offset " << table->offset<< " get " << recv_from_vec[dest] << " rows from " << dest << " offset " << offset << std::endl;
    table->offset += recv_from_vec[dest];
  }
  MPI_Win_fence(0, input->win);
  input->release_window();
  // copy local rows from input to new table
  void* dest_ptr = table->payload_ptr() + rowsize * get_row_index_rank[rank];
  memcpy(dest_ptr, in->range_ptr(rank), send_to_vec[rank] * rowsize);

  LOG(INFO) << rank << "=" << recv_row_count;

  table->offset = recv_row_count * rowsize;
  table->row_count = recv_row_count;

  MPI_Type_free(&row_type);
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

const std::shared_ptr<arrow::RecordBatch> processors::java(std::shared_ptr<arrow::RecordBatch> batch, std::string class_name, std::shared_ptr<node> node) {
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