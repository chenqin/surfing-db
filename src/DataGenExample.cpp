/*
 * Copyright Chen Qin on 12/30/22.
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

#include <chrono>
#include <csignal>
#include <fmt/core.h>
#include <future>
#include <glog/logging.h>
#include <iostream>
#include <jni.h>
#include <omp.h>
#include <rapidjson/document.h>
#include <torch/csrc/distributed/c10d/ProcessGroupMPI.hpp>
#include <torch/csrc/distributed/c10d/Work.hpp>
#include <torch/torch.h>
#include "connector/datagen.h"
#include "meta/node.h"
#include "table/processors.h"

#define FLUSH_DIR "/tmp/"
#define BATCH_SIZE 2250

using namespace surfingdb::meta;
using namespace surfingdb::table::schema;
using namespace surfingdb::table;
using namespace surfingdb::connector;
using namespace std;

/**
 * https://stackoverflow.com/questions/440133/how-do-i-create-a-random-alpha-numeric-string-in-c
 */
std::string random_string(size_t length) {
  auto randchar = []() -> char {
    const char charset[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = (sizeof(charset) - 1);
    return charset[rand() % max_index];
  };
  std::string str(length, 0);
  std::generate_n(str.begin(), length, randchar);
  return str;
}

volatile std::sig_atomic_t terminal_signal;
void signal_handler(int signal) {
  std::cout << "user exit program";
  terminal_signal = signal;
}

// Define a Convolutional Module
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

void process(c10::intrusive_ptr<c10d::ProcessGroupMPI> pg, int rank, int numranks) {
  // ensure data provider is not gpu rank
  CHECK_NE(rank, 0);
  // Read train dataset
  const char* kDataRoot = "data/mnist/";
  auto train_dataset = torch::data::datasets::MNIST(kDataRoot)
                         .map(torch::data::transforms::Normalize<>(0.1307, 0.3081))
                         .map(torch::data::transforms::Stack<>());

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
}

void train(c10::intrusive_ptr<c10d::ProcessGroupMPI> pg, int rank, int numranks) {
  CHECK_EQ(rank, 0);
  /**
   * @brief ensure gpu node stay in rank 0
   *
   */
  auto cuda_available = torch::cuda::is_available();
  torch::Device device(cuda_available ? torch::kCUDA : torch::kCPU);
  // TRAINING
  // Read train dataset
  const char* kDataRoot = "data/mnist/";
  auto train_dataset = torch::data::datasets::MNIST(kDataRoot)
                         .map(torch::data::transforms::Normalize<>(0.1307, 0.3081))
                         .map(torch::data::transforms::Stack<>());

  // Distributed Random Sampler
  auto data_sampler = torch::data::samplers::DistributedRandomSampler(
    train_dataset.size().value(), numranks, rank, false);

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

/** run this program with
 * mpirun -np 12 ./Test
 * @return
 */
int main(int argc, char** argv) {

  google::InstallFailureSignalHandler();
  google::InitGoogleLogging(argv[0]);
  // define how data will be stored, as rows in a table
  RowSchema r;
  SchemaUtils::initField(r, "timestamp", RowType::LONG, sizeof(long));
  SchemaUtils::initField(r, "host", RowType::STRING, 64);
  SchemaUtils::initField(r, "metricName", RowType::STRING, MAX_STR_LEN);
  SchemaUtils::initListField(r, "metricValues", RowType::DOUBLE, 2, sizeof(DOUBLE_TYPE));
  SchemaUtils::initMapField(r, "meta", RowType::STRING, RowType::STRING, 1, 32, 64);

  /**
   * @brief initial constructors
   * node -> single executor binding to MPI rank, number of node determined by mpi processes
   * mschema -> row based MPI friendly schema defined to encode/decode table/row in O(1) time
   * con -> data connector ingess running on a number of nodes micro batching data pullers
   */
  auto pg = c10d::ProcessGroupMPI::createProcessGroupMPI();
  const auto node = std::make_shared<surfingdb::meta::node>(&argc, &argv);

  const auto schema_ptr = std::make_shared<mschema>(r);
  const auto con = std::make_unique<DataGenConnector>(node);

  std::signal(SIGTERM | SIGINT, signal_handler);

  /**
   * @brief
   * show case consumer send data async to ranks not pulling data
   * so that while other workers working on shuffle or post shuffle stages
   * consumer ranks can async send data to other ranks
   * jump to next iteration and get next batch ready
   */
  bool produce = node->rank != 0;
  node->setissubscriber(&produce);
  auto partitioner = [](size_t key, int rank, int world) {
    int base = world % 2 == 0 ? world - 1 : world;
    int dest = key % world;
    /**
     * @brief avoid use GPU rank in preprocessing
     *
     */
    return dest == 0 ? dest + 1 : dest;
  };

  while (terminal_signal == 0) {
    if (node->rank == 0) std::cout << "iteration" << std::endl;
    /**
     * @brief import pyarrow
     */
    // arrow::py::import_pyarrow();c10d::ProcessGroupMPI::createProcessGroupMPI();

    const size_t intial_row_count = node->rank * BATCH_SIZE;
    size_t total_row_count = intial_row_count;
    double start = MPI_Wtime();
    // ingest, copy rows to local table memory with fixed offsets
    const auto t1 = con->consume_batch(intial_row_count, 1000, schema_ptr, [](const char* payload, const mschema& out) {
      auto row = std::make_shared<mrow>(std::make_shared<mschema>(out));
      Value p;

      p.p_val.long_val = 1;
      row->write(out.fields.at(0), p);

      p.p_val.string_val = "hello_host";
      row->write(out.fields.at(1), p);

      p.p_val.string_val = random_string(16);
      row->write(out.fields.at(2), p);

      p.p_val.double_val = 0.1;
      std::vector<PValue> lval;
      lval.push_back(p.p_val);
      lval.push_back(p.p_val);
      p.list_value = lval;
      row->write(out.fields.at(3), p);
      PValue key;
      PValue value;
      key.string_val = random_string(MAX_STR_LEN - 1);
      value.string_val = random_string(MAX_STR_LEN - 1);
      std::pair<PValue, PValue> pair;
      pair.first = key;
      pair.second = value;
      p.map_value.insert(pair);
      row->write(out.fields.at(4), p);
      return row;
    });

    /**
     * pass each row in mtable, if return true, add to new table with schema ptr
     * release t1 mtable in the end
     */
    auto t2 = processors::map(t1, schema_ptr, [](mrow& in, mrow& out, const mschema& out_schema) {
      for (const auto& f : out_schema.fields) {
        Value v;
        in.read(f, v);
        out.write(f, v);
      }
      return true;
    });

    start = MPI_Wtime();
    auto t4 = processors::shuffle(t2, schema_ptr->fields.at(2), partitioner);
    auto end = MPI_Wtime();
    // std::cout << " shuffle time = " << (end - start) << " rank = " << node->rank << " ingestor = " << node->getissubscriber() << std::endl;
    start = MPI_Wtime();
    /**
     * verify shuffle row placement to right worker (aka MPI rank)
     */
    t4->verifyShuffle(schema_ptr->fields.at(2), partitioner);

    auto t41 = processors::java(t4, "Bridge");

    /**
     * @brief shuffle again with another field with same partitioner
     *
     */
    auto t5 = processors::shuffle(t4, schema_ptr->fields.at(4), partitioner);
    t5->verifyShuffle(schema_ptr->fields.at(4), partitioner);

    /**
     * @brief read data from java
     *
     */
    //auto t51 = processors::java(t5, "MyBridge");

    /**
     * @brief rest of worker load data convert to tensor and send to gpu rank 0
     * gpu rank 0 only train
     */
    if (node->rank != 0 && node->world > 1) {
      process(pg, node->rank, node->world);
    } else {
      train(pg, node->rank, node->world);
    }
  }
  return terminal_signal;
}
