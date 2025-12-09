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
#include <arrow/c/helpers.h>
#include <omp.h>
#include <cstdlib>

#include "mtable.h"
#include "utils.h"

namespace matcha {
namespace table {

/**
 * @brief scan through each row, apply transform function, append to output
 * table if return true
 *
 * @param in  input micro batch table
 * @param out_schema_ptr  output micro batch table schema
 * @param transform transform function with simple type
 * @return std::shared_ptr<mtable>  outputtable
 */
std::shared_ptr<mtable> processors::map(
    std::shared_ptr<mtable> input, std::shared_ptr<mschema> out_schema_ptr,
    std::function<bool(mrow&, mrow&, const mschema&)> transform) {
  auto out_table_ptr =
      std::make_shared<mtable>(input->getNodePtr(), out_schema_ptr,
                               input->row_count * out_schema_ptr->rowSize());
  for (size_t i = 0; i < input->row_count; i++) {
    auto in_row = input->readRow(i);
    /**
     * @brief use out table memory to avoid memcpy
     */
    mrow shaddlow_out_row(out_table_ptr->getSchema(),
                          out_table_ptr->payload_ptr() + out_table_ptr->offset);
    bool append =
        transform(*in_row.get(), shaddlow_out_row, *out_schema_ptr.get());
    CHECK_EQ(shaddlow_out_row.schema_sig(), out_schema_ptr->signature());

    /**
     * @brief update offset and row_count
     *
     */
    if (append) {
      out_table_ptr->row_count++;
      out_table_ptr->offset += out_table_ptr->getSchema()->rowSize();
      CHECK_LE(out_table_ptr->row_count, input->row_count);
      CHECK_LE(out_table_ptr->offset,
               input->row_count * out_schema_ptr->rowSize());
    } else {
      /**
       * @brief reset memory
       *
       */
      memset(shaddlow_out_row.payload_ptr(), 0,
             out_table_ptr->getSchema()->rowSize());
    }
  }
  return out_table_ptr;
}

void processors::reduce(
    std::shared_ptr<mtable> in_ptr, Field& field,
    std::shared_ptr<
        std::unordered_map<Value, std::shared_ptr<mrow>, ValueHasher>>
        result_ptr,
    std::shared_ptr<mschema> result_schema_ptr,
    std::function<void(Value&, std::vector<std::unique_ptr<mrow>>&,
                       std::shared_ptr<mrow>&)>
        reducer) {
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
        result_ptr->insert({key, std::make_shared<mrow>(result_schema_ptr)});
      }
      std::shared_ptr<mrow> row = result_ptr->at(key);
      reducer(key, val_list, row);
    }
  }
}


std::shared_ptr<arrow::RecordBatch> processors::shuffle_one_side(
    const arrow::RecordBatchVector& batches,
    std::string filedname, std::function<size_t(size_t, int, int)> partitioner,
    int rank, int world) {
  auto start = MPI_Wtime();
  size_t send_to_vec[world];
  size_t recv_from_vec[world];
  MPI_Win window;
  std::vector<std::shared_ptr<arrow::Buffer>> send_buffers(world);
  std::vector<std::shared_ptr<arrow::Buffer>> recv_buffers(world);

  int total_batches = 0;
  int local_batches = batches.size();
  MPI_Allreduce(&local_batches, &total_batches, 1, MPI_INT, MPI_SUM,
                MPI_COMM_WORLD);
  double discovery_elapsed = MPI_Wtime() - start;
  LOG(INFO) << "[rank " << rank << "/" << world
            << "] shuffle_one_side discovered " << total_batches
            << " total batch(es) (" << local_batches << " local) for field '"
            << filedname << "' in " << discovery_elapsed << "s";
  // nothing to shuffle
  CHECK_GT(total_batches, 0);

  auto schema = batches[0]->schema();
  size_t local_org_row = 0;
  size_t total_bytes = 0;

  // mark start offset in send window for each recive to pull from
  size_t send_to_vec_offset[world];
  // mark start offset in each sender buffer current rank can pull from with
  // mpi_get
  size_t recv_from_vec_offset[world];
  start = MPI_Wtime();
  for (int j = 0; j < world; j++) {
    auto send_to_dest_table =
        utils::group_two(batches, filedname, partitioner, j, rank, world);
    // Optional spill to disk for large partitions when configured
    const char* spill_dir = std::getenv("MATCHA_SPILL_DIR");
    const char* spill_rows_env = std::getenv("MATCHA_SPILL_MAX_ROWS");
    if (spill_dir && send_to_dest_table->num_rows() > 0) {
      int64_t max_rows = spill_rows_env ? std::strtoll(spill_rows_env, nullptr, 10) : 100000;
      auto maybe_paths = utils::SpillRecordBatchToIpcFiles(send_to_dest_table, spill_dir, max_rows);
      if (maybe_paths.ok()) {
        auto& paths = maybe_paths.ValueUnsafe();
        if (!paths.empty()) {
          LOG(INFO) << "[rank " << rank << "/" << world
                    << "] spilled partition for dest " << j << " to "
                    << paths.size() << " file(s) under " << spill_dir;
        }
      }
    }
    auto buffer = utils::serialize(send_to_dest_table);
    // store data get pulled from rank j to send_buffers[j]
    send_buffers[j] = buffer;
    local_org_row += send_to_dest_table->num_rows();
  }

  double group_elapsed = MPI_Wtime() - start;
  LOG(INFO) << "[rank " << rank << "/" << world
            << "] grouped " << local_org_row << " row(s) into " << world
            << " partition(s) in " << group_elapsed << "s";

  for (int j = 0; j < world; j++) {
    send_to_vec[j] = send_buffers[j]->size();
    send_to_vec_offset[j] =
        j == 0 ? 0 : send_to_vec_offset[j - 1] + send_to_vec[j - 1];
    total_bytes += send_to_vec[j];
  }
  start = MPI_Wtime();
  // recv_from_vec[j] is the total bytes rank can pull from rank j
  MPI_Alltoall(&send_to_vec, 1, MPI_UNSIGNED_LONG, recv_from_vec, 1,
               MPI_UNSIGNED_LONG, MPI_COMM_WORLD);
  // recv_from_vec_offset[j] is start offset to pull from rank j window with
  // RDMA
  MPI_Alltoall(&send_to_vec_offset, 1, MPI_UNSIGNED_LONG, recv_from_vec_offset,
               1, MPI_UNSIGNED_LONG, MPI_COMM_WORLD);
  double offset_elapsed = MPI_Wtime() - start;
  size_t total_recv_bytes = 0;
  for (int j = 0; j < world; j++) {
    total_recv_bytes += recv_from_vec[j];
  }
  LOG(INFO) << "[rank " << rank << "/" << world
            << "] exchanged partition sizes in " << offset_elapsed
            << "s; sending " << total_bytes << " byte(s), expecting "
            << total_recv_bytes << " byte(s)";

  /**
   * @brief contact send buffers into one window buffer
   *
   */
  start = MPI_Wtime();
  auto window_buffer = arrow::ConcatenateBuffers(send_buffers).ValueOrDie();
  CHECK_EQ(total_bytes, window_buffer->size());
  // Nothing to exchange; return empty batch.
  if (total_bytes == 0) {
    LOG(INFO) << "[rank " << rank << "/" << world
              << "] shuffle_one_side has no data to exchange";
    return arrow::RecordBatch::Make(schema, 0,
                                     std::vector<std::shared_ptr<arrow::Array>>{});
  }
  double concat_elapsed = MPI_Wtime() - start;
  LOG(INFO) << "[rank " << rank << "/" << world
            << "] concatenated send buffers (" << total_bytes
            << " byte(s)) in " << concat_elapsed << "s";

  start = MPI_Wtime();
  MPI_Win_create(window_buffer->mutable_data(), window_buffer->size(),
                 sizeof(char), MPI_INFO_NULL, MPI_COMM_WORLD, &window);

  // starts non overlap mpi_gets
  MPI_Win_fence(0, window);

  for (int dest = 0; dest < world; dest++) {
    if (dest == rank || recv_from_vec[dest] == 0) continue;
    recv_buffers[dest] =
        arrow::AllocateBuffer(recv_from_vec[dest]).ValueOrDie();
    MPI_Aint offset = recv_from_vec_offset[dest];
    CHECK_EQ(MPI_SUCCESS, MPI_Get(recv_buffers[dest]->mutable_data(),
                                  recv_from_vec[dest], MPI_CHAR, dest, offset,
                                  recv_from_vec[dest], MPI_CHAR, window));
  }

  MPI_Win_fence(0, window);

  MPI_Win_free(&window);
  double movement_elapsed = MPI_Wtime() - start;
  LOG(INFO) << "[rank " << rank << "/" << world
            << "] completed RDMA window transfer in " << movement_elapsed
            << "s";

  arrow::RecordBatchVector arrow_tables;
  start = MPI_Wtime();
  for (int i = 0; i < world; i++) {
    if (i == rank || recv_from_vec[i] == 0) continue;
    auto _buffer = recv_buffers[i];
    auto arrow_table = utils::deserialize(_buffer, schema);
    arrow_tables.push_back(arrow_table);
  }

  if (send_to_vec[rank] > 0) {
    auto arrow_table = utils::deserialize(send_buffers[rank], schema);
    arrow_tables.push_back(arrow_table);
  }
  double deser_elapsed = MPI_Wtime() - start;
  LOG(INFO) << "[rank " << rank << "/" << world
            << "] deserialized " << arrow_tables.size()
            << " partition batch(es) in " << deser_elapsed << "s";

  size_t total_rows = 0;
  for (auto& batch : arrow_tables) {
    total_rows += batch->num_rows();
  }
  auto out_batch = utils::merge(arrow_tables, schema);
  // Optional post-shuffle spill based on row threshold
  const char* post_spill_dir = std::getenv("MATCHA_POST_SHUFFLE_SPILL_DIR");
  const char* post_spill_min_rows_env = std::getenv("MATCHA_POST_SHUFFLE_SPILL_MIN_ROWS");
  const char* post_spill_max_rows_env = std::getenv("MATCHA_POST_SHUFFLE_SPILL_MAX_ROWS");
  if (post_spill_dir) {
    int64_t min_rows = post_spill_min_rows_env ? std::strtoll(post_spill_min_rows_env, nullptr, 10) : LLONG_MAX;
    int64_t max_rows = post_spill_max_rows_env ? std::strtoll(post_spill_max_rows_env, nullptr, 10) : 100000;
    if (out_batch->num_rows() >= min_rows) {
      auto maybe_paths = utils::SpillRecordBatchToIpcFiles(out_batch, post_spill_dir, max_rows);
      if (maybe_paths.ok()) {
        auto& paths = maybe_paths.ValueUnsafe();
        if (!paths.empty()) {
          LOG(INFO) << "[rank " << rank << "/" << world
                    << "] post-shuffle spill wrote " << paths.size()
                    << " file(s) to " << post_spill_dir;
        }
      }
    }
  }
  LOG(INFO) << "[rank " << rank << "/" << world
            << "] shuffle_one_side produced " << total_rows
            << " row(s) across " << arrow_tables.size() << " batch(es)";
  return out_batch;

/*
  start = MPI_Wtime();
  size_t shuffle_row_sum = 0;
  size_t org_row_sum = 0;

  MPI_Allreduce(&local_org_row, &org_row_sum, 1, MPI_UNSIGNED_LONG, MPI_SUM,
                MPI_COMM_WORLD);

  MPI_Allreduce(&total_rows, &shuffle_row_sum, 1, MPI_UNSIGNED_LONG, MPI_SUM,
                MPI_COMM_WORLD);

  // check before and after shuffle total row count equal across all ranks
  CHECK_EQ(shuffle_row_sum, org_row_sum);
  LOG(INFO) << "data check " << MPI_Wtime() - start << " seconds " << rank; 
*/
}

std::shared_ptr<arrow::RecordBatch> processors::shuffle_two_side(
    const arrow::RecordBatchVector& batches,
    std::string filedname, std::function<size_t(size_t, int, int)> partitioner,
    int rank, int world) {
  size_t send_to_vec[world], recv_from_vec[world];
  MPI_Request sends[world];
  MPI_Request recvs[world];
  MPI_Status statuses[world];
  std::vector<std::shared_ptr<arrow::Buffer>> send_buffers(world);

  int total_batches = 0;
  int local_batches = batches.size();
  double phase_start = MPI_Wtime();
  MPI_Allreduce(&local_batches, &total_batches, 1, MPI_INT, MPI_SUM,
                MPI_COMM_WORLD);
  double discovery_elapsed = MPI_Wtime() - phase_start;
  LOG(INFO) << "[rank " << rank << "/" << world
            << "] shuffle_two_side discovered " << total_batches
            << " total batch(es) (" << local_batches << " local) for field '"
            << filedname << "' in " << discovery_elapsed << "s";

  // nothing to shuffle
  CHECK_GT(total_batches, 0);

  size_t total_rows = 0;
  for (auto& batch : batches) {
    total_rows += batch->num_rows();
  }
  size_t orgin_row_sum = 0;
  phase_start = MPI_Wtime();
  MPI_Allreduce(&total_rows, &orgin_row_sum, 1, MPI_UNSIGNED_LONG, MPI_SUM,
                MPI_COMM_WORLD);
  double row_reduce_elapsed = MPI_Wtime() - phase_start;
  LOG(INFO) << "[rank " << rank << "/" << world
            << "] shuffle_two_side accounted for " << total_rows
            << " local row(s); global total " << orgin_row_sum
            << " row(s) computed in " << row_reduce_elapsed << "s";

  auto schema = batches[0]->schema();
  phase_start = MPI_Wtime();
  for (int j = 0; j < world; j++) {
    auto send_to_dest_table =
        utils::group_two(batches, filedname, partitioner, j, rank, world);
    CHECK(send_to_dest_table->Validate().ok());
    auto buffer = utils::serialize(send_to_dest_table);
    send_buffers[j] = buffer;
    send_to_vec[j] = buffer->size();
  }

  double partition_elapsed = MPI_Wtime() - phase_start;
  size_t total_send_bytes = 0;
  for (int j = 0; j < world; j++) {
    total_send_bytes += send_to_vec[j];
  }
  LOG(INFO) << "[rank " << rank << "/" << world
            << "] shuffle_two_side partitioned " << total_rows
            << " row(s) into " << world << " buffer(s) totaling "
            << total_send_bytes << " byte(s) in " << partition_elapsed << "s";

  phase_start = MPI_Wtime();
  MPI_Alltoall(&send_to_vec, 1, MPI_UNSIGNED_LONG, recv_from_vec, 1,
               MPI_UNSIGNED_LONG, MPI_COMM_WORLD);

  size_t recv_bytes = 0;
  size_t tranfer_bytes_rank_index[world];
  for (int i = 0; i < world; i++) {
    recv_bytes += recv_from_vec[i];
    tranfer_bytes_rank_index[i] =
        (i == 0) ? 0 : recv_from_vec[i - 1] + tranfer_bytes_rank_index[i - 1];
  }
  double size_exchange_elapsed = MPI_Wtime() - phase_start;
  LOG(INFO) << "[rank " << rank << "/" << world
            << "] shuffle_two_side exchanged partition sizes in "
            << size_exchange_elapsed << "s; sending " << total_send_bytes
            << " byte(s) and expecting " << recv_bytes << " byte(s)";
  auto maybe_buffer = arrow::AllocateBuffer(recv_bytes);
  CHECK(maybe_buffer.ok());
  std::shared_ptr<arrow::Buffer> recv_buffer =
      std::move(maybe_buffer.ValueOrDie());
  int send_count = 0, recv_count = 0;

  phase_start = MPI_Wtime();
  for (int i = 0; i < world; i++) {
    int send_to_rank = i;
    if (send_to_vec[send_to_rank] > 0) {
      int tag = rank * world + send_to_rank;
      MPI_Isend(send_buffers[send_to_rank]->data(), send_to_vec[send_to_rank],
                MPI_CHAR, send_to_rank, tag, MPI_COMM_WORLD,
                &sends[send_count++]);
    }
    int recv_from_rank = i;
    if (recv_from_vec[recv_from_rank] > 0) {
      CHECK_LE(tranfer_bytes_rank_index[recv_from_rank], recv_bytes);
      int tag = recv_from_rank * world + rank;
      MPI_Irecv(recv_buffer->mutable_data() +
                    tranfer_bytes_rank_index[recv_from_rank],
                recv_from_vec[recv_from_rank], MPI_CHAR, recv_from_rank, tag,
                MPI_COMM_WORLD, &recvs[recv_count++]);
    }
  }
  MPI_Waitall(send_count, sends, statuses);
  MPI_Waitall(recv_count, recvs, statuses);
  double transfer_elapsed = MPI_Wtime() - phase_start;
  LOG(INFO) << "[rank " << rank << "/" << world
            << "] shuffle_two_side completed " << send_count
            << " send(s) and " << recv_count << " recv(s) moving "
            << recv_bytes << " byte(s) in " << transfer_elapsed << "s";

  for (int i = 0; i < world; i++) {
    if (sends[i] != MPI_REQUEST_NULL) MPI_Request_free(&sends[i]);
    if (recvs[i] != MPI_REQUEST_NULL) MPI_Request_free(&recvs[i]);
  }

  arrow::RecordBatchVector arrow_tables;
  phase_start = MPI_Wtime();
  for (int i = 0; i < world; i++) {
    if (recv_from_vec[i] > 0) {
      auto _buffer = arrow::SliceBuffer(
          recv_buffer, tranfer_bytes_rank_index[i], recv_from_vec[i]);
      auto arrow_table = utils::deserialize(_buffer, schema);
      CHECK(arrow_table->Validate().ok());
      arrow_tables.push_back(arrow_table);
    }
  }
  total_rows = 0;
  for (auto& batch : arrow_tables) {
    total_rows += batch->num_rows();
  }
  double deser_elapsed = MPI_Wtime() - phase_start;
  LOG(INFO) << "[rank " << rank << "/" << world
            << "] shuffle_two_side deserialized " << arrow_tables.size()
            << " batch(es) totaling " << total_rows << " row(s) in "
            << deser_elapsed << "s";

  size_t shuffle_row_sum = 0;
  MPI_Allreduce(&total_rows, &shuffle_row_sum, 1, MPI_UNSIGNED_LONG, MPI_SUM,
                MPI_COMM_WORLD);
  // check before and after shuffle total row count equal
  CHECK_EQ(shuffle_row_sum, orgin_row_sum);
  auto out_batch2 = utils::merge(arrow_tables, schema);
  // Optional post-shuffle spill based on row threshold
  {
    const char* post_spill_dir = std::getenv("MATCHA_POST_SHUFFLE_SPILL_DIR");
    const char* post_spill_min_rows_env = std::getenv("MATCHA_POST_SHUFFLE_SPILL_MIN_ROWS");
    const char* post_spill_max_rows_env = std::getenv("MATCHA_POST_SHUFFLE_SPILL_MAX_ROWS");
    if (post_spill_dir) {
      int64_t min_rows = post_spill_min_rows_env ? std::strtoll(post_spill_min_rows_env, nullptr, 10) : LLONG_MAX;
      int64_t max_rows = post_spill_max_rows_env ? std::strtoll(post_spill_max_rows_env, nullptr, 10) : 100000;
      if (out_batch2->num_rows() >= min_rows) {
        auto maybe_paths = utils::SpillRecordBatchToIpcFiles(out_batch2, post_spill_dir, max_rows);
        if (maybe_paths.ok()) {
          auto& paths = maybe_paths.ValueUnsafe();
          if (!paths.empty()) {
            LOG(INFO) << "[rank " << rank << "/" << world
                      << "] post-shuffle spill wrote " << paths.size()
                      << " file(s) to " << post_spill_dir;
          }
        }
      }
    }
  }
  LOG(INFO) << "[rank " << rank << "/" << world
            << "] shuffle_two_side produced " << total_rows
            << " row(s) across " << arrow_tables.size() << " batch(es)";
  return out_batch2;
}

std::shared_ptr<arrow::RecordBatch> processors::shuffle(
    std::shared_ptr<arrow::RecordBatch>& batch,
    std::string field_name, std::function<size_t(size_t, int, int)> partitioner,
    bool singleside, int rank, int world) {
  arrow::RecordBatchVector input;
  input.push_back(batch);
  auto start = MPI_Wtime();
  auto out = singleside ? processors::shuffle_one_side(input, field_name,
                                                       partitioner, rank, world)
                        : processors::shuffle_two_side(
                              input, field_name, partitioner, rank, world);
  double shuffle_elapsed = MPI_Wtime() - start;
  const char* mode = singleside ? "one-side" : "two-side";
  int64_t input_rows = batch ? batch->num_rows() : 0;
  int64_t output_rows = out ? out->num_rows() : 0;
  LOG(INFO) << "[rank " << rank << "/" << world
            << "] shuffle(" << mode << ") by '" << field_name << "' finished in "
            << shuffle_elapsed << "s; input rows=" << input_rows
            << ", output rows=" << output_rows;
  // TODO: we observed a bug in tcp MPI_GET that lead to one row of garbage
  return out;
}


std::pair<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::RecordBatch>>
processors::cogroup(
    const arrow::RecordBatchVector& left,
    const arrow::RecordBatchVector& right,
    std::string field_name,
    std::function<size_t(size_t, int, int)> partitioner,
    bool singleside,
    int rank,
    int world) {
  CHECK(world >= 1);
  CHECK(rank >= 0 && rank < world);
  // Both sides are allowed to be non-empty; if any side is empty, return an
  // empty batch with that side's schema if derivable, otherwise an empty batch
  // matching the other side's schema if keys are the same. For simplicity,
  // require at least one batch present on each side globally.
  int local_left = left.size();
  int local_right = right.size();
  int total_left = 0, total_right = 0;
  MPI_Allreduce(&local_left, &total_left, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
  MPI_Allreduce(&local_right, &total_right, 1, MPI_INT, MPI_SUM, MPI_COMM_WORLD);
  CHECK(total_left >= 0);
  CHECK(total_right >= 0);

  std::shared_ptr<arrow::RecordBatch> left_out;
  std::shared_ptr<arrow::RecordBatch> right_out;

  if (total_left > 0) {
    if (singleside) {
      left_out = processors::shuffle_one_side(left, field_name, partitioner, rank, world);
    } else {
      left_out = processors::shuffle_two_side(left, field_name, partitioner, rank, world);
    }
  } else {
    // If no left globally, synthesize empty with right schema if available.
    if (total_right > 0) {
      auto schema = right[0]->schema();
      left_out = arrow::RecordBatch::MakeEmpty(schema).ValueOrDie();
    } else {
      // both empty; return trivially empty using a dummy schema
      auto schema = arrow::schema({});
      left_out = arrow::RecordBatch::MakeEmpty(schema).ValueOrDie();
    }
  }

  if (total_right > 0) {
    if (singleside) {
      right_out = processors::shuffle_one_side(right, field_name, partitioner, rank, world);
    } else {
      right_out = processors::shuffle_two_side(right, field_name, partitioner, rank, world);
    }
  } else {
    if (total_left > 0) {
      auto schema = left[0]->schema();
      right_out = arrow::RecordBatch::MakeEmpty(schema).ValueOrDie();
    } else {
      auto schema = arrow::schema({});
      right_out = arrow::RecordBatch::MakeEmpty(schema).ValueOrDie();
    }
  }

  return {left_out, right_out};
}

std::pair<std::shared_ptr<arrow::RecordBatch>, std::shared_ptr<arrow::RecordBatch>>
processors::cogroup(std::shared_ptr<arrow::RecordBatch>& left,
                    std::shared_ptr<arrow::RecordBatch>& right,
                    std::string field_name,
                    std::function<size_t(size_t, int, int)> partitioner,
                    bool singleside,
                    int rank,
                    int world) {
  arrow::RecordBatchVector lv, rv;
  if (left) lv.push_back(left);
  if (right) rv.push_back(right);
  return processors::cogroup(lv, rv, field_name, partitioner, singleside, rank, world);
}


std::map<std::string, jobject> java_instances;

std::shared_ptr<arrow::RecordBatch> processors::java(
    const std::shared_ptr<arrow::RecordBatch>& batch, std::string class_name,
    JNIEnv* env) {
  const jclass bridge = env->FindClass(class_name.c_str());

  CHECK_NOTNULL(bridge);
  struct ArrowSchema arrowSchemaIn, arrowSchemaOut;
  struct ArrowArray arrowArrayIn, arrowArrayOut;
  const jmethodID invoke_method = env->GetStaticMethodID(
      bridge, std::string(BRIDGE_METHOD_NAME).c_str(), "(JJJJ)V");
  CHECK_NOTNULL(invoke_method);

  /**
   * @brief export schema and data
   *
   */
  auto schema = batch->schema();
  arrow::ExportSchema(*schema.get(), &arrowSchemaIn);
  arrow::ExportRecordBatch(*batch.get(), &arrowArrayIn, &arrowSchemaIn);
  /**
   * @brief invoke java method, passing pointers
   *
   */
  env->CallStaticVoidMethod(
      bridge, invoke_method,
      static_cast<jlong>(reinterpret_cast<uintptr_t>(&arrowSchemaIn)),
      static_cast<jlong>(reinterpret_cast<uintptr_t>(&arrowArrayIn)),
      static_cast<jlong>(reinterpret_cast<uintptr_t>(&arrowSchemaOut)),
      static_cast<jlong>(reinterpret_cast<uintptr_t>(&arrowArrayOut)));

  if (env->ExceptionCheck()) {
    LOG(ERROR) << "fail to call jni";
    env->DeleteLocalRef(bridge);
    ArrowArrayRelease(&arrowArrayIn);
    ArrowArrayRelease(&arrowArrayOut);
    ArrowSchemaRelease(&arrowSchemaIn);
    ArrowSchemaRelease(&arrowSchemaOut);
    return java(batch, class_name, env);
  }
  env->DeleteLocalRef(bridge);
  /**
   * @brief import schema and data from java
   *
   */
  const auto resultImportVectorSchemaRoot =
      arrow::ImportRecordBatch(&arrowArrayOut, &arrowSchemaOut);
  std::shared_ptr<arrow::RecordBatch> out =
      resultImportVectorSchemaRoot.ValueOrDie();
  ArrowArrayRelease(&arrowArrayIn);
  ArrowArrayRelease(&arrowArrayOut);
  ArrowSchemaRelease(&arrowSchemaIn);
  ArrowSchemaRelease(&arrowSchemaOut);
  return std::move(out);
}

arrow::RecordBatchVector processors::jni(
    const arrow::RecordBatchVector& batch,
    std::string class_name, JNIEnv* env, int rank) {
  auto start = MPI_Wtime();
  arrow::RecordBatchVector out;
  int processed_batches = 0;
  int64_t input_rows = 0;
  int64_t output_rows = 0;
  for (int i = 0; i < batch.size(); i++) {
    auto& b = batch[i];
    //std::cout << b->ToString();
    CHECK(b->Validate().ok());
    if ( b->num_rows() == 0 ) continue;
    processed_batches++;
    input_rows += b->num_rows();
    auto result = java(b, class_name, env);
    if (result) {
      output_rows += result->num_rows();
    }
    out.push_back(std::move(result));
  }
  if (!batch.empty()) {
    double elapsed = MPI_Wtime() - start;
    LOG(INFO) << "[rank " << rank
              << "] jni call to '" << class_name << "' processed "
              << processed_batches << "/" << batch.size() << " batch(es); input rows="
              << input_rows << ", output rows=" << output_rows << ", elapsed="
              << elapsed << "s";
  }
  return std::move(out);
}

std::shared_ptr<arrow::RecordBatch> processors::java(
    const std::shared_ptr<arrow::RecordBatch>& batch, std::string class_name,
    JNIEnv* env, jclass* clz, jobject* instance) {
  CHECK_NOTNULL(*clz);
  jmethodID constructor = env->GetMethodID(*clz, "<init>", "()V");

  CHECK_NOTNULL(*instance);
  struct ArrowSchema arrowSchemaIn, arrowSchemaOut;
  struct ArrowArray arrowArrayIn, arrowArrayOut;
  const jmethodID invoke_method = env->GetMethodID(
      *clz, std::string(BRIDGE_METHOD_NAME).c_str(), "(JJJJ)V");
  CHECK_NOTNULL(invoke_method);

  /**
   * @brief export schema and data
   *
   */
  auto schema = batch->schema();
  arrow::ExportSchema(*schema.get(), &arrowSchemaIn);
  arrow::ExportRecordBatch(*batch.get(), &arrowArrayIn, &arrowSchemaIn);
  /**
   * @brief invoke java method, passing pointers
   *
   */
  jlong schema_in_addres =
      static_cast<jlong>(reinterpret_cast<jlong>(&arrowSchemaIn));
  jlong array_in_addres =
      static_cast<jlong>(reinterpret_cast<jlong>(&arrowArrayIn));
  jlong schema_out_addres =
      static_cast<jlong>(reinterpret_cast<jlong>(&arrowSchemaOut));
  jlong array_out_addres =
      static_cast<jlong>(reinterpret_cast<jlong>(&arrowArrayOut));
  env->CallVoidMethod(*instance, invoke_method, schema_in_addres,
                      array_in_addres, schema_out_addres, array_out_addres);
  /**
   * @brief import schema and data from java
   *
   */
  const auto resultImportVectorSchemaRoot =
      arrow::ImportRecordBatch(&arrowArrayOut, &arrowSchemaOut);
  std::shared_ptr<arrow::RecordBatch> out =
      resultImportVectorSchemaRoot.ValueOrDie();
  ArrowArrayRelease(&arrowArrayIn);
  ArrowArrayRelease(&arrowArrayOut);
  ArrowSchemaRelease(&arrowSchemaIn);
  ArrowSchemaRelease(&arrowSchemaOut);
  return std::move(out);
}

}  // namespace table
}  // namespace matcha
