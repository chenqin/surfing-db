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

#include <glog/logging.h>

#include <chrono>
#include <future>
#include <iostream>
#include <string>
#include <experimental/random>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include "mabs/mabs_constants.h"
#include "mabs/mabs_types.h"
#include "connector/kafka.h"
#include "meta/node.h"
#include "table/processors.h"
#include "table/utils.h"

using namespace matcha::table::schema;
using matcha::meta::node;
using namespace matcha::table;
using namespace matcha::connector;
using namespace std::chrono;
using namespace apache::thrift::transport;
using namespace apache::thrift::protocol;

volatile std::sig_atomic_t terminal_signal;
void signal_handler(int signal) {
  std::cout << "user exit program";
  terminal_signal = signal;
}
/** run this program with
 * mpirun -np 12 ./MainTest
 * @return
 */
int main(int argc, char** argv) {
  google::InstallFailureSignalHandler();
  google::InitGoogleLogging(argv[0]);

  //  create node of cluster
  // auto pg = c10d::ProcessGroupMPI::createProcessGroupMPI();
  CHECK_GT(argc, 1);
  std::string jar(argv[1]);
  //  create node of cluster
  // auto pg = c10d::ProcessGroupMPI::createProcessGroupMPI();
  const auto node = std::make_shared<matcha::meta::node>(&argc, &argv, jar);
  /**
   * @brief define topic schema
   *struct MabsMetrics {

  // Time of metric generation in epoch time
  1: required i64 timestamp,

  // Tagstring containing service level info. (i.e nimbus_uuid, host_type...)
  2: optional string service_tags,

  // Tagstring containing node level info. (i.e host, pod)
  3: optional string node_tags,

  // Store the counters.
  4: optional map<string, i64> counters,

  // Store the gauges.
  5: optional map<string, double> gauges,

  // Store the histograms.
  6: optional map<string, string> histograms,

  // service name.
  7: optional string service_name,

  // Store the double counters.
  8: optional map<string, double> double_counters,

}
   */
  RowSchema r;
  SchemaUtils::initField(r, "timestamp", RowType::LONG, sizeof(long));
  SchemaUtils::initField(r, "service_tags", RowType::STRING, MAX_STR_LEN);
  SchemaUtils::initField(r, "node_tags", RowType::STRING, MAX_STR_LEN);
  SchemaUtils::initMapField(r, "counters", RowType::STRING, RowType::LONG, 1024, MAX_STR_LEN, sizeof(long));
  SchemaUtils::initMapField(r, "gauges", RowType::STRING, RowType::DOUBLE, 1024, MAX_STR_LEN, sizeof(float));
  SchemaUtils::initMapField(r, "histograms", RowType::STRING, RowType::STRING, 1024, MAX_STR_LEN, MAX_STR_LEN);
  SchemaUtils::initField(r, "service_name", RowType::STRING,  MAX_STR_LEN);
  SchemaUtils::initMapField(r, "double_counters", RowType::STRING, RowType::DOUBLE, 1024, MAX_STR_LEN, sizeof(float));
  const std::shared_ptr<mschema> schema_ptr = std::make_shared<mschema>(r);

  /**
   * pull every 2 seconds
   */
  int batch = 50;
  int interval = 2; //thanks to large fan out within jni, we need to reduce mini batch size per pull
  int world = node->world;

  size_t total = 0;
  srand(std::time(nullptr));

  auto start = MPI_Wtime();
  std::string group_id = "mabv1";

  std::signal(SIGTERM | SIGINT, signal_handler);
  MabsMetrics a;

  auto metrics_prod = KafkaConnector(
      node, "kafka-source", batch, interval, schema_ptr, {"mabs_raw"},
      "/var/serverset/discovery.metricskafka05.prod", group_id, false);
  metrics_prod.setDeser(
      [&schema_ptr](
          const void* payload, size_t len,
          std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders) {
        std::shared_ptr<TMemoryBuffer> strBuffer(new TMemoryBuffer((uint8_t*) payload, len));
        std::shared_ptr<TBinaryProtocol> binaryProtcol(new TBinaryProtocol(strBuffer));
        MabsMetrics obj;
        obj.read(binaryProtcol.get());

        PValue v1,v2,v3,v4,v5,v6,v7,v8;
        Value placeholder;
        v1.long_val = obj.timestamp;
        v2.string_val = obj.service_tags;
        v3.string_val = obj.node_tags;
        Value v4map;
        for(auto& b : obj.counters) {
          PValue key;
          key.string_val = b.first;
          PValue val;
          val.long_val = (long) b.second;
          v4map.map_value.insert({key, val});
        }
        Value v5map;
        for(auto& b : obj.gauges) {
          PValue key;
          key.string_val = b.first;
          PValue val;
          val.double_val = (float) b.second;
          v5map.map_value.insert({key, val});
        }
        Value v6map;
        for(auto& b : obj.gauges) {
          PValue key;
          key.string_val = b.first;
          PValue val;
          val.string_val = b.second;
          v6map.map_value.insert({key, val});
        }
        v7.string_val = obj.service_name;
        Value v8map;
        for(auto& b : obj.gauges) {
          PValue key;
          key.string_val = b.first;
          PValue val;
          val.double_val = (float) b.second;
          v8map.map_value.insert({key, val});
        }
        utils::append(builders.at(0).get(), schema_ptr->fields.at(0), v1, placeholder); 
        utils::append(builders.at(1).get(), schema_ptr->fields.at(1), v2, placeholder);
        utils::append(builders.at(2).get(), schema_ptr->fields.at(2), v3, placeholder); 
        utils::append(builders.at(3).get(), schema_ptr->fields.at(3), v4, v4map);
        utils::append(builders.at(4).get(), schema_ptr->fields.at(4), v5, v5map);
        utils::append(builders.at(5).get(), schema_ptr->fields.at(5), v6, v6map);
        utils::append(builders.at(6).get(), schema_ptr->fields.at(6), v7, placeholder);
        utils::append(builders.at(7).get(), schema_ptr->fields.at(7), v8, v8map);
        return 1;
      });

  auto t1 = std::async(std::launch::async, [&metrics_prod, &terminal_signal] {
    metrics_prod.run(terminal_signal);
  });

  std::mutex record_mutex, jni_mutex;
  std::condition_variable record_batch_not_full, record_batch_not_empty;
  std::condition_variable jni_batch_not_full, jni_batch_not_empty;
  arrow::RecordBatchVector record_batch, jni_batch;
  size_t max_batch_size = 1000;

  auto t2 = std::async(std::launch::async, [&] {
    JavaVM* jvm;
    JNIEnv* env;
    JavaVMInitArgs vm_args;
    JavaVMOption options[10];
    std::string s = "-Djava.class.path=" + jar;
    int opt = 0;
    options[opt++].optionString = (char*) s.c_str();
    options[opt++].optionString = (char*) "-XX:+UseG1GC"; // concurrent GC
    options[opt++].optionString = (char*) "-XX:+ExplicitGCInvokesConcurrent";
    options[opt++].optionString = (char*) "-XX:+UseStringDeduplication";
    options[opt++].optionString = (char*) "-XX:MaxGCPauseMillis=200";
    options[opt++].optionString = (char*) "-XX:InitiatingHeapOccupancyPercent=30";
    options[opt++].optionString = (char*) "-XX:MaxTenuringThreshold=8";
    options[opt++].optionString = (char*) "-XX:MaxDirectMemorySize=1024m";
    //options[1].optionString = "-verbose:jni";
    vm_args.version = JNI_VERSION_1_8;
    vm_args.nOptions = opt;
    vm_args.options = options;
    vm_args.ignoreUnrecognized = JNI_TRUE;
    int status = JNI_CreateJavaVM(&jvm, (void**)&env, &vm_args);
    if (status < 0) {
      std::cerr << "\n<<<<< Unable to Launch JVM >>>>>\n"
                << std::endl;
      env = nullptr;
    }
    CHECK_NOTNULL(env);
    while (terminal_signal == 0) {
      // read avaliable records from reshard queue
      std::unique_lock<std::mutex> lock(record_mutex);
      arrow::RecordBatchVector ret = std::move(record_batch);
      record_batch.clear();
      CHECK_EQ(record_batch.size(), 0);
      lock.unlock();
      record_batch_not_full.notify_one();

      auto keyed_metric = processors::jni(
        ret, "org/surfing/drsquirrel/MabsMetric", env, node->rank);
      
      std::unique_lock<std::mutex> lock1(jni_mutex);
      jni_batch.insert(jni_batch.end(), keyed_metric.begin(), keyed_metric.end());
      lock1.unlock();
    }
  });
  
  auto partitioner = [](size_t key, int rank, int world) {
    int random_number = std::experimental::randint(0, world);
    return random_number;
  };

  size_t total_count = 0;
  double total_time = 0;
 auto metric_schema =
      arrow::schema({
        arrow::field("key", arrow::utf8()), 
        arrow::field("timestamp", arrow::int64()) , 
        arrow::field("payload", arrow::utf8())
      });
  std::shared_ptr<arrow::RecordBatch> keystate = nullptr;
  //auto keystate = arrow::RecordBatch::MakeEmpty(metric_schema).ValueOrDie();

  while (terminal_signal == 0) {
    // simulate a delay to decode and handle kafka batch
    auto start = MPI_Wtime();
    long kafka_row_count = 0;
    long metric_row_count = 0;
    // kafka consumer
    auto schema = utils::toArrow(schema_ptr);
    auto m1 = metrics_prod.poll_once(schema);
    total_count += m1->num_rows();
    auto reshard = processors::shuffle(m1, "timestamp", partitioner, true, node->rank, node->world);

    // write reshard columnar table to queue
    std::unique_lock<std::mutex> lock(record_mutex);
    record_batch_not_full.wait(lock, [&] { 
      size_t total = 0;
      for(auto& batch : record_batch){
        total += batch->num_rows();
      }
      return total < max_batch_size;
    });
    record_batch.push_back(reshard);
    lock.unlock();


    std::unique_lock<std::mutex> lock1(jni_mutex);
    auto m = utils::merge(jni_batch, metric_schema);
    jni_batch.clear();
    CHECK_EQ(jni_batch.size(), 0);
    lock1.unlock();
    
    metric_row_count = m->num_rows();
    auto metric_start = MPI_Wtime();
    auto shard_metric = processors::shuffle(m, "key", [](size_t key, int rank, int world) {
      return key % world;
    }, true, node->rank, node->world);
    arrow::RecordBatchVector in = {shard_metric};
    
    if (keystate != nullptr) {
      in.push_back(keystate);
    }
    
    arrow::RecordBatchVector out;
    for (auto& b : in) {
      auto col = b->GetColumnByName("key");
      auto ts = b->GetColumnByName("timestamp");
      for (int i = 0; i < b->num_rows(); i++) {
        auto t = ts->GetScalar(i).ValueOrDie();
        auto bc = (arrow::Int64Scalar*)t.get();
        if( bc->value < 1000 * (MPI_Wtime() - 10)) continue;
        out.push_back(b->Slice(i, 1));
      }
    }
    
    if (out.size() == 0) {
      keystate = nullptr;
    } else {
      //auto ta = arrow::Table::FromRecordBatches(out);
      //CHECK(ta.ok());
      //keystate = ta.ValueOrDie()->CombineChunksToBatch().ValueOrDie();
    }
    
    size_t global_row_count = 0;
    MPI_Allreduce(&total_count, &global_row_count, 1, MPI_UNSIGNED_LONG,
                  MPI_SUM, MPI_COMM_WORLD);
    total_time += (MPI_Wtime() - start);
    if (node->rank == 0 && global_row_count > 0 ) LOG(INFO) << "kafka e2e " << global_row_count/total_time << " qps"<< std::endl;
    //utils::jvmGC(node->env);
  }
  t1.wait();
  t2.wait();
  return terminal_signal;
}
