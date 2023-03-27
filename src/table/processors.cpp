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

struct Model : torch::nn::Module {
  Model()
    : conv1(torch::nn::Conv2dOptions(1, 10, 5)),
      conv2(torch::nn::Conv2dOptions(10, 20, 5)),
      fc1(320, 50),
      fc2(50, 10) {
    register_module("conv1", conv1);
    register_module("conv2", conv2);
    register_module("conv2_drop", conv2_drop);
    register_module("fc1", fc1);
    register_module("fc2", fc2);
  }

  torch::Tensor forward(torch::Tensor x) {
    x = torch::relu(torch::max_pool2d(conv1->forward(x), 2));
    x = torch::relu(
      torch::max_pool2d(conv2_drop->forward(conv2->forward(x)), 2));
    x = x.view({ -1, 320 });
    x = torch::relu(fc1->forward(x));
    x = torch::dropout(x, 0.5, is_training());
    x = fc2->forward(x);
    return torch::log_softmax(x, 1);
  }

  torch::nn::Conv2d conv1;
  torch::nn::Conv2d conv2;
  torch::nn::Dropout2d conv2_drop;
  torch::nn::Linear fc1;
  torch::nn::Linear fc2;
};

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

void processors::mnist(c10::intrusive_ptr<c10d::ProcessGroupMPI> pg, std::shared_ptr<mtable> in) {
  CHECK_NOTNULL(pg);
  CHECK_NOTNULL(in);
  CHECK_NOTNULL(in->getSchema());
  // Read train dataset
  const char* kDataRoot = "data/mnist/";
  auto train_dataset = torch::data::datasets::MNIST(kDataRoot)
                         .map(torch::data::transforms::Normalize<>(0.1307, 0.3081))
                         .map(torch::data::transforms::Stack<>());
  int rank = pg->getRank();
  int numranks = pg->getSize();

  // Distributed Random Sampler
  auto data_sampler = torch::data::samplers::DistributedRandomSampler(
    train_dataset.size().value(), numranks, rank, false);

  if (pg->getRank() != 0) {
    // ensure data provider is not gpu rank
    CHECK_NE(rank, 0);
    // Distributed Random Sampler
    auto data_sampler = torch::data::samplers::DistributedRandomSampler(
      train_dataset.size().value(), numranks, rank, false);

    auto num_train_samples_per_proc = train_dataset.size().value() / numranks;

    // Generate dataloader
    auto total_batch_size = 64;
    auto batch_size_per_proc = total_batch_size / numranks; // effective batch size in each processor
    auto data_loader = torch::data::make_data_loader(
      std::move(train_dataset), data_sampler, batch_size_per_proc);

    // setting manual seed
    torch::manual_seed(0);
    // Number of epochs
    size_t num_epochs = 10;

    for (size_t epoch = 1; epoch <= num_epochs; ++epoch) {
      size_t num_correct = 0;

      for (auto& batch : *data_loader) {
        std::vector<torch::Tensor> rank_batch_data = { batch.data };
        std::vector<torch::Tensor> rank_batch_target = { batch.target };
        auto work1 = pg->send(rank_batch_data, 0, rank);
        auto work2 = pg->send(rank_batch_target, 0, rank);
        work1->wait();
        work2->wait();
      } // end batch loader
    }   // end epoch
  } else {
    CHECK_EQ(rank, 0);
  /**
   * @brief ensure gpu node stay in rank 0
   *
   */
  auto cuda_available = torch::cuda::is_available();
  torch::Device device(cuda_available ? torch::kCUDA : torch::kCPU);
  // TRAINING
  // Read train dataset

  auto num_train_samples_per_proc = train_dataset.size().value();

  // Generate dataloader
  auto total_batch_size = 64;
  auto batch_size_per_proc = total_batch_size / numranks; // effective batch size in each processor
  auto data_loader = torch::data::make_data_loader(
    std::move(train_dataset), data_sampler, batch_size_per_proc);

  // setting manual seed
  torch::manual_seed(0);

  auto model = std::make_shared<Model>();
  model->to(device);

  auto learning_rate = 1e-2;

  torch::optim::SGD optimizer(model->parameters(), learning_rate);

  // Number of epochs
  size_t num_epochs = 10;

  for (size_t epoch = 1; epoch <= num_epochs; ++epoch) {
    size_t num_correct = 0;
    auto start = MPI_Wtime();
    for (auto& batch : *data_loader) {
      /**
       * @brief collect all batch data from CPU ranks and feed into
       *
       */
      std::vector<std::vector<torch::Tensor>> all_data, all_target;
      std::vector<c10::intrusive_ptr<c10d::Work>> data_workers, target_workers;

      for (int i = 0; i < numranks; i++) {
        /**
         * @brief async recv all mini batch from all workers
         *        put rank 0 data on first index
         */
        all_data.push_back({ batch.data.clone() });
        all_target.push_back({ batch.target.clone() });
        /**
         * @brief setup async recv data and target from rank i
         *
         */
        if (i > 0) {
          auto work1 = pg->recv(all_data.at(i), i, i);
          auto work2 = pg->recv(all_target.at(i), i, i);
          data_workers.push_back(work1);
          target_workers.push_back(work2);
        }
      }

      /**
       * @brief async feed batch and target into gpu
       *
       */
      std::vector<at::Tensor> ips;
      std::vector<at::Tensor> ops;
      std::mutex g_mutex;
      std::condition_variable cv;
      bool ready = false;

      std::future<size_t> gpu_task = std::async(std::launch::async, [&]{ 
        int j = 0;

        while(j < numranks) {
          std::unique_lock lk(g_mutex);
          cv.wait(lk, []{return true;});
          // Reset gradients
          model->zero_grad();

          auto ip = ips.at(j);
          auto op = ops.at(j);
          // Execute forward pass
          auto prediction = model->forward(ip);
          auto loss = torch::nll_loss(torch::log_softmax(prediction, 1), op);

          // Backpropagation
          loss.backward();

          //  Update parameters
          optimizer.step();

          auto guess = prediction.argmax(1);
          num_correct += torch::sum(guess.eq_(op)).item<int64_t>();
          j++;
          lk.unlock();
        }
        return num_correct; 
      });

      for (int i = 0; i < numranks; i++) {
        if (i > 0) {
          data_workers.at(i - 1)->wait();
          batch.data = all_data.at(i).at(0);
        }

        auto ip = batch.data.to(device);
        ip = ip.to(torch::kF32);
        ips.push_back(ip);
        if (i > 0) {
          target_workers.at(i - 1)->wait();
          batch.target = all_target.at(i).at(0);
        }
        auto op = batch.target.to(device).squeeze();
        op = op.to(torch::kLong);
        ops.push_back(op);
        /**
        * one sample data and target ready, notify training thread
        */
        std::lock_guard lk(g_mutex);
        ready = true;
        cv.notify_one();
      }
      gpu_task.wait();
    } // end batch loader

    /**
      * @brief model sync, move grad from CUDA to CPU to run MPI
      * 1) do sum of all grad values across all gpus ranks
      * 2) avg grad and send back to all gpu ranks
      */
    std::vector<c10::intrusive_ptr<c10d::Work>> sync_worker;
    for (auto& param : model->named_parameters()) {
      std::vector<torch::Tensor> send_temp = {param.value().grad().clone().to(torch::kCPU)};
      std::vector<torch::Tensor> recv_temp = {param.value().grad().clone().to(torch::kCPU)};
      auto sender = pg->send(send_temp, 0, 0);
      sync_worker.push_back(sender);
    }

    for (auto& param : model->named_parameters()) {
      std::vector<torch::Tensor> recv_temp = {param.value().grad().clone().to(torch::kCPU)};
      auto reciver = pg->recv(recv_temp, 0, 0);
      sync_worker.push_back(reciver);
    }
    /**
      * @brief wait till gpu grad sync complete
      * 
      * @param sync_worker 
      */
    for(auto& worker : sync_worker) {
      worker->wait();
    }

    auto accuracy = 100.0 * num_correct / num_train_samples_per_proc;
    std::cout << "Accuracy in rank " << rank << " in epoch " << epoch << " - "
              << accuracy << " with "<< num_train_samples_per_proc/ (MPI_Wtime() - start) << " qps" << std::endl;

  } // end epoch

  auto test_dataset = torch::data::datasets::MNIST(
                        kDataRoot, torch::data::datasets::MNIST::Mode::kTest)
                        .map(torch::data::transforms::Normalize<>(0.1307, 0.3081))
                        .map(torch::data::transforms::Stack<>());

  auto num_test_samples = test_dataset.size().value();
  auto test_loader = torch::data::make_data_loader(
    std::move(test_dataset), num_test_samples);

  model->eval(); // enable eval mode to prevent backprop

  size_t num_correct = 0;

  for (auto& batch : *test_loader) {
    auto ip = batch.data.to(device);
    auto op = batch.target.to(device).squeeze();

    // convert to required format
    ip = ip.to(torch::kF32);
    op = op.to(torch::kLong);

    auto prediction = model->forward(ip);

    auto loss = torch::nll_loss(torch::log_softmax(prediction, 1), op);

    std::cout << "Test loss - " << loss.item<float>() << std::endl;

    auto guess = prediction.argmax(1);

    num_correct += torch::sum(guess.eq_(op)).item<int64_t>();

  } // end test loader

  std::cout << "Num correct - " << num_correct << std::endl;
  std::cout << "Test Accuracy - " << 100.0 * num_correct / num_test_samples
            << std::endl;
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

  /**
   * @brief import schema and data from java
   *
   */
  const auto resultImportVectorSchemaRoot = arrow::ImportRecordBatch(&arrowArrayOut, &arrowSchemaOut);
  std::shared_ptr<arrow::RecordBatch> recordBatch = resultImportVectorSchemaRoot.ValueOrDie();
  return utils::fromArrow(recordBatch, units, node);
  release_malloced_array(&arrowArrayIn);
  release_malloced_array(&arrowArrayOut);
  release_malloced_type(&arrowSchemaIn);
  release_malloced_type(&arrowSchemaOut);
}

} // namespace table
} // namespace surfingdb