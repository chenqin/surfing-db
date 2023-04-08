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
    mrow shaddlow_out_row(out_table_ptr->getSchema(), out_table_ptr->buffer->mutable_data() + out_table_ptr->offset);
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

std::shared_ptr<mtable> processors::shuffle(std::shared_ptr<mtable> input, Field& f, std::function<size_t(size_t key, int rank, int world)> partitioner) {
  auto schema_ptr = input->getSchema();
  auto node_ptr = input->getNodePtr();
  int rank = node_ptr->rank;
  int world = node_ptr->world;
  size_t rowsize = schema_ptr->rowSize();

  auto in = input->placement_sort(f, partitioner);
  /**
   * verify all workers in same stage
   */
  node_ptr->forward();
  /**
   * register and commit schema row size unit to all workers
   */
  MPI_Datatype row_type;
  MPI_Type_contiguous(rowsize, MPI_CHAR, &row_type);
  MPI_Type_commit(&row_type);

  MPI_Request sends[node_ptr->world];
  MPI_Request recvs[node_ptr->world];
  MPI_Status statuses[node_ptr->world];

  size_t send_to_vec[world], recv_from_vec[world];

  for (int j = 0; j < world; j++) {
    size_t send_to_i = in->range_row_size(j);
    send_to_vec[j] = send_to_i;
  }

  MPI_Alltoall(&send_to_vec, 1, MPI_UNSIGNED_LONG, recv_from_vec, 1, MPI_UNSIGNED_LONG, MPI_COMM_WORLD);

  size_t recv_row_count = 0;
  size_t transfered_row_index_rank[world];
  /**
   * @brief transfered_row_index_rank determins placement of newly transfered row
   * we don't differentiate local move v.s remote as MPI
   *
   */
  for (int i = 0; i < world; i++) {
    recv_row_count += recv_from_vec[i];
    transfered_row_index_rank[i] = (i == 0) ? 0 : recv_from_vec[i - 1] + transfered_row_index_rank[i - 1];
  }
  auto table = std::make_shared<mtable>(node_ptr, schema_ptr, recv_row_count * rowsize);

  CHECK_LT((recv_row_count * rowsize), MEM_PAGE_SIZE); // no more than given table size

  int send_count = 0, recv_count = 0;

  for (int i = 0; i < node_ptr->world; i++) {
    int send_rank_offset = i;
    /**
     * @brief current rank has data sending to send_rank_offset
     *
     */
    if (send_to_vec[send_rank_offset] > 0) {
      // LOG(INFO) << node_ptr->rank << "-> " << i << " size " << in->range_row_size(i);
      /**
       * @brief tag is unqiue value from sender to reciever with two dim array
       *
       */
      int tag = rank * world + send_rank_offset;
      MPI_Isend(in->range_ptr(send_rank_offset), send_to_vec[send_rank_offset], row_type, send_rank_offset, tag, MPI_COMM_WORLD, &sends[send_count++]);
    }
    int recv_rank_offset = i;
    /**
     * @brief current rank has data recieveing form recv_rank_offset
     *
     */
    if (recv_from_vec[recv_rank_offset] > 0) {
      CHECK_LE(transfered_row_index_rank[recv_rank_offset], recv_row_count);
      // LOG(INFO) << node_ptr->rank << " <- " << i << " size " << recv_lens[i];
      /**
       * @brief tag is unqiue value from sender to reciever with two dim array
       * matching sender side
       */
      int tag = recv_rank_offset * world + rank;
      MPI_Irecv(table->payload_ptr() + transfered_row_index_rank[recv_rank_offset] * rowsize, recv_from_vec[recv_rank_offset], row_type, recv_rank_offset, tag, MPI_COMM_WORLD, &recvs[recv_count++]);
    }
  }
  MPI_Waitall(send_count, sends, statuses);
  MPI_Waitall(recv_count, recvs, statuses);
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

const std::shared_ptr<mtable> processors::java(std::shared_ptr<mtable> input, std::string class_name, std::map<std::string, uint64_t> units) {
  auto node = input->getNodePtr();
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
  auto batch = utils::toArrow(input);
  auto schema_ptr = utils::toArrow(input->getSchema());
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
    return java(input, class_name, units);
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
  return utils::fromArrow({std::move(recordBatch)}, units, node);
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