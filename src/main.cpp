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

#include <future>
#include <glog/logging.h>
#include <iostream>
#include <omp.h>
#include "meta/node.h"
#include "table/processors.h"
#include "connector/kafka.h"

#define FLUSH_DIR "/tmp/"

using namespace surfingdb::table::schema;
using surfingdb::meta::node;
using namespace surfingdb::table;
using namespace surfingdb::connector;

void transform(const RowBuffer& in, RowBuffer& out) {
  out = in;
}

/** run this program with
 * mpirun -np 12 ./MainTest
 * @return
 */
int main(int argc, char** argv) {
  google::InstallFailureSignalHandler();

  //google::InitGoogleLogging(argv[0]);
  // create node of cluster
  const auto node = std::make_shared<surfingdb::meta::node>(&argc, &argv);

  RowSchema r;
  r.fields = std::vector<surfingdb::table::schema::Field>();

  Field field1, field2, field3, field4, field5, field6, field7;

  SchemaUtils::initField(field1, "a", RowType::INT, sizeof(int));
  SchemaUtils::initField(field2, "b", RowType::LONG, sizeof(long));
  SchemaUtils::initField(field3, "c", RowType::BOOL, sizeof(bool));
  SchemaUtils::initField(field4, "d", RowType::DOUBLE, sizeof(DOUBLE_TYPE));
  SchemaUtils::initField(field5, "e", RowType::STRING, MAX_STR_LEN);

  SchemaUtils::initListField(field6, "l", RowType::DOUBLE, 24, sizeof(DOUBLE_TYPE));
  SchemaUtils::initMapField(field7, "m", RowType::STRING, RowType::LONG, 10, MAX_STR_LEN, sizeof(long));

  r.fields.push_back(field1);
  r.fields.push_back(field2);
  r.fields.push_back(field3);
  r.fields.push_back(field4);
  r.fields.push_back(field5);
  r.fields.push_back(field6);
  r.fields.push_back(field7);

  std::shared_ptr<TableSchema> schema_ptr = std::make_shared<TableSchema>(r);

  Value v1, v2, v3, v4, v5, v6, v7;
  v1.p_val.int_val = node->rank + 1;

  v2.p_val.long_val = 4;

  v3.p_val.bool_val = true;

  v4.p_val.double_val = 0.1f;

  v5.p_val.string_val = "hello";
  PValue p;
  p.double_val = 0.1;
  std::vector<PValue> lval;
  lval.push_back(p);
  v6.list_value = lval;
  PValue key, value;
  key.string_val = "hello";
  value.long_val = 1l;
  std::pair<PValue, PValue> pair;
  pair.first = key;
  pair.second = value;
  v7.map_value.insert(pair);

  surfingdb::table::RowBuffer b(schema_ptr);
  b.write(field1, v1);
  b.write(field2, v2);
  b.write(field3, v3);
  b.write(field4, v4);
  b.write(field5, v5);
  b.write(field6, v6);
  b.write(field7, v7);

  /**
   * ingestion
   */
  TempTable tsed(node, schema_ptr);
  mtable msed(node, schema_ptr, 3000 * schema_ptr->rowSize());
  /**
  * ingestion
  */
  size_t i;
  for (i = 0; i < 3000; i++) {
    //tsed.ingest(b);
    //msed.appendRow(b);
  }

  node->forward(); // stage 1 broadcast data

  // stage 2 run k_mean
  std::vector<Field> fields;
  fields.push_back(field4);
  fields.resize(1);

  KMeanOperator op(1, fields, 100);
  //tsed.process(op);

  LOG(INFO) << "xgb operator"
            << " process " << node->rank << " has " << tsed.count() << " rows";
  XGBParameters parameters;
  parameters.tree_method = "exact";
  parameters.objective = "reg:linear";
  parameters.model_path = "/tmp/xgb.bin";
  parameters.min_child_weight = 1;
  parameters.gamma = 0.1;
  parameters.max_depth = 2;
  parameters.verbosity = true;
  parameters.eval_metric = "error";
  //processors::xgb(msed, fields, field4, parameters);

  Value v;
  v.p_val.int_val = 1;
  b.write(field1, v);
  int rows = 150;
  size_t total = 0;
  srand(std::time(nullptr));
  auto start = MPI_Wtime();
  auto results_ptr = std::make_shared<std::unordered_map<Value , std::shared_ptr<RowBuffer>, ValueHasher>>();
//#pragma omp parallel num_threads(3) shared(results_ptr, total)
  {

  	std::string v = "xenon_metrics_prod";
  	std::string brokers = "datakafka08001:9092,datakafka08002:9092,datakafka08003:9092";
  	auto consumer = KafkaConnector();
    consumer.init(v, brokers);
    auto t1 = std::make_shared<mtable>(node, schema_ptr, rows * schema_ptr->rowSize());

    while (true) {

      //simulate a delay to decode and handle kafka batch
      std::this_thread::sleep_for(std::chrono::microseconds(rand()%10));
      //should inference compact schema from source (e.g handle kafka events)
	    auto t1 = std::make_shared<mtable>(node, schema_ptr, rows * schema_ptr->rowSize());
			auto messages = consumer.consume_batch(rows, 100 ,schema_ptr);
      for (auto m : messages) {
        Value v;
        v.p_val.int_val = (int) MPI_Wtime()*100;
        m.write(field1, v);
        t1->appendRow(m);
      }
      messages.clear();

      auto t2 = processors::map(t1, schema_ptr, [&](RowBuffer in, RowBuffer out) {
        for (auto f : schema_ptr->fields) {
          Value v;
          in.read(f, v);
          out.write(f, v);
        }
      });
      t1->release();
      auto t3 = processors::shuffle(t2, field1);
      //t2->release();
      t3->verify(field1);
      processors::reduce(t3, field1, results_ptr, schema_ptr, [=](Value& key,std::vector<std::unique_ptr<RowBuffer>>& vals, std::shared_ptr<RowBuffer>& result){
        Value v;
        result->read(field1,v);
        v.p_val.long_val += vals.size();
        result->write(field1, v);
      });

#pragma omp critical
      total += t3->row_count;
      if(omp_get_thread_num() == 0) {
        LOG(INFO) << (total / (MPI_Wtime() - start)) * schema_ptr->rowSize() / (1024 * 1024) << "MB ps on " << node->rank;
      }
    }
  }
}
