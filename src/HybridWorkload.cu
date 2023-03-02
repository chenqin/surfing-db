#include <arrow/device.h>
#include <arrow/gpu/cuda_api.h>
#include <arrow/gpu/cuda_context.h>
#include <c10/cuda/CUDAStream.h>
#include <chrono>
#include <fmt/core.h>
#include <future>
#include <glog/logging.h>
#include <iostream>
#include <omp.h>
#include <rapidjson/document.h>
#include <stdio.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <torch/torch.h>
#include "connector/datagen.h"
#include "meta/node.h"
#include "table/processors.h"
#include "table/utils.h"

#define BATCH_SIZE 2000

using namespace surfingdb::meta;
using namespace surfingdb::table::schema;
using namespace surfingdb::table;
using namespace surfingdb::connector;
using namespace std;
using namespace torch;
using namespace arrow::cuda;
using namespace arrow;

__global__ void saxpy(int n, float a, float* x, float* y) {
  int i = blockIdx.x * blockDim.x + threadIdx.x;
  if (i < n) y[i] = a * x[i] + y[i];
}

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

/**
 * @brief example that calls pytorch cuda streams api as well as calling cuda low level API
 *
 * @param input
 * @return std::shared_ptr<mtable>
 */
std::shared_ptr<mtable> pytorch(std::shared_ptr<mtable> input) {
  if (!torch::cuda::is_available()) return nullptr;
  ;
  /*
    Tensor tensor0 = torch::ones({ 2, 2 }, torch::device(torch::kCUDA));
    // get a new CUDA stream from CUDA stream pool on device 0
    at::cuda::CUDAStream myStream = at::cuda::getStreamFromPool();
    // set current CUDA stream from default stream to `myStream` on device 0
    at::cuda::setCurrentCUDAStream(myStream);
    // sum() on tensor0 uses `myStream` as current CUDA stream
    tensor0.sum();

    // get the default CUDA stream on device 0
    at::cuda::CUDAStream defaultStream = at::cuda::getDefaultCUDAStream();
    // set current CUDA stream back to default CUDA stream on device 0
    at::cuda::setCurrentCUDAStream(defaultStream);
    // sum() on tensor0 uses `defaultStream` as current CUDA stream
    tensor0.sum();
  */

  int N = input->row_count;
  surfingdb::meta::Field f = input->getSchema()->fields.at(4);

  float *x, *y, *d_x, *d_y;
  x = (float*)malloc(N * sizeof(float));
  y = (float*)malloc(N * sizeof(float));

  cudaMalloc(&d_x, N * sizeof(float));
  cudaMalloc(&d_y, N * sizeof(float));

  for (int i = 0; i < N; i++) {
    auto row = input->readRow(i);
    Value v;
    row->read(f, v);
    x[i] = 0.1f;
    y[i] = 0.2f;
  }

  cudaMemcpy(d_x, x, N * sizeof(float), cudaMemcpyHostToDevice);
  cudaMemcpy(d_y, y, N * sizeof(float), cudaMemcpyHostToDevice);

  // Perform SAXPY on N elements
  saxpy<<<(N + 255) / 256, 256>>>(N, 2.0f, d_x, d_y);

  cudaMemcpy(y, d_y, N * sizeof(float), cudaMemcpyDeviceToHost);

  float maxError = 0.0f;
  for (int i = 0; i < N; i++)
    maxError = max(maxError, abs(y[i] - 4.0f));
  printf("Max error: %f\n", maxError);

  cudaFree(d_x);
  cudaFree(d_y);
  free(x);
  free(y);
  return input;
}

int main(int argc, char** argv) {

  google::InstallFailureSignalHandler();
  google::InitGoogleLogging(argv[0]);

  // define how data will be stored, as rows in a table
  RowSchema r;
  SchemaUtils::appendElements(r, "timestamp", RowType::LONG, 1);
  SchemaUtils::appendElements(r, "host", RowType::STRING, 1);
  SchemaUtils::appendElements(r, "metricName", RowType::STRING, 1);
  // min, max of metricValue
  SchemaUtils::appendElements(r, "metricValues", RowType::DOUBLE, 2);
  // user defined meta data pair
  SchemaUtils::appendPairs(r, "meta", RowType::STRING, RowType::STRING, 1);

  /**
   * @brief initial constructors
   * node -> single executor binding to MPI rank, number of node determined by mpi processes
   * mschema -> row based MPI friendly schema defined to encode/decode table/row in O(1) time
   * con -> data connector ingess running on a number of nodes micro batching data pullers
   */
  const auto node = std::make_shared<surfingdb::meta::node>(&argc, &argv);
  const auto ptr = std::make_shared<mschema>(r);
  const auto con = std::make_unique<DataGenConnector>(node);
  std::function<void(int, float, float*, float*)> func = saxpy;

  std::signal(SIGTERM | SIGINT, signal_handler);

  /**
   * @brief
   * show case consumer send data async to ranks not pulling data
   * so that while other workers working on shuffle or post shuffle stages
   * consumer ranks can async send data to other ranks
   * jump to next iteration and get next batch ready
   */
  bool produce = node->rank % 2 == 0;
  node->setissubscriber(&produce);
  auto partitioner = [](size_t key, int rank, int world) {
    int base = world % 2 == 0 ? world - 1 : world;
    int dest = key % base;
    /**
     * @brief dest is subscriber to data ingestion
     *
     */
    if (dest % 2 == 0) {
      if (dest + 1 > world - 1) {
        dest = dest - 1;
      } else {
        dest = dest + 1;
      }
    }
    CHECK_GE(dest, 0);
    CHECK_LT(dest, world);
    return dest;
  };

  while (terminal_signal == 0) {
    const size_t intial_row_count = node->rank * BATCH_SIZE;
    size_t total_row_count = intial_row_count;
    double start = MPI_Wtime();
    // ingest, copy rows to local table memory with fixed offsets
    const auto t1 = con->consume_batch(intial_row_count, 1000, ptr, [](const char* payload, const mschema& out) {
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
    auto t2 = processors::map(t1, ptr, [](mrow& in, mrow& out, const mschema& out_schema) {
      for (const auto& f : out_schema.fields) {
        Value v;
        in.read(f, v);
        out.write(f, v);
      }
      return true;
    });
    auto t4 = processors::shuffle(t2, ptr->fields.at(2), partitioner);
    t4->verifyShuffle(ptr->fields.at(2), partitioner);
    auto t41 = processors::java(t4, "Bridge");

    auto t5 = pytorch(t4);

    CudaDeviceManager* manager_;
    std::shared_ptr<CudaDevice> device_;
    std::shared_ptr<CudaMemoryManager> mm_;
    std::shared_ptr<CudaContext> context_;
    std::shared_ptr<arrow::Device> cpu_device_;
    std::shared_ptr<MemoryManager> cpu_mm_;
    manager_ = CudaDeviceManager::Instance().ValueOrDie();
    device_ = manager_->GetDevice(0).ValueOrDie();
    context_ = device_->GetContext().ValueOrDie();
    mm_ = AsCudaMemoryManager(device_->default_memory_manager()).ValueOrDie();
    cpu_device_ = arrow::CPUDevice::Instance();
    cpu_mm_ = cpu_device_->default_memory_manager();

  }
  return terminal_signal;
}