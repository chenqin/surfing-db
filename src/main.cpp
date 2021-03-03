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
#include <rapidjson/document.h>

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

  SchemaUtils::initField(field1, "timestamp", RowType::LONG, sizeof(long));
  SchemaUtils::initField(field2, "host", RowType::STRING, MAX_STR_LEN);
  SchemaUtils::initField(field3, "metricName", RowType::STRING, MAX_STR_LEN);
  SchemaUtils::initField(field4, "metricValue", RowType::DOUBLE, sizeof(DOUBLE_TYPE));

  r.fields.push_back(field1);
  r.fields.push_back(field2);
  r.fields.push_back(field3);
  r.fields.push_back(field4);

  std::shared_ptr<TableSchema> schema_ptr = std::make_shared<TableSchema>(r);

  int rows = 150;
  size_t total = 0;
  srand(std::time(nullptr));
  auto start = MPI_Wtime();
  auto results_ptr = std::make_shared<std::unordered_map<Value , std::shared_ptr<RowBuffer>, ValueHasher>>();

#pragma omp parallel num_threads(CONCURRENCY) shared(results_ptr, total)
  {

  	std::string v = "xenon_metrics_prod";
  	std::string brokers = "datakafka08001:9092,datakafka08002:9092,datakafka08003:9092";
  	auto consumer = KafkaConnector(v, brokers);
    auto t1 = std::make_shared<mtable>(node, schema_ptr, rows * schema_ptr->rowSize());

    while (true) {

      //simulate a delay to decode and handle kafka batch
      std::this_thread::sleep_for(std::chrono::microseconds(rand()%10));
      //should inference compact schema from source (e.g handle kafka events)
	    auto t1 = std::make_shared<mtable>(node, schema_ptr, rows * schema_ptr->rowSize());

	    auto messages = consumer.consume_batch(rows, 1000 , schema_ptr, [=](const char *payload, std::shared_ptr<surfingdb::meta::TableSchema> schema_ptr) -> std::shared_ptr<RowBuffer> {
						auto r = std::make_shared<RowBuffer>(schema_ptr);
						rapidjson::Document document;
						rapidjson::ParseResult ok = document.Parse((const char *) payload);
						if (ok) {
							for (auto f : schema_ptr->fields) {
								Value v;
								if (f.type == RowType::LONG) {
									v.p_val.long_val = document[f.name.c_str()].GetInt64();
									r->write(f, v);
								} else if (f.type == RowType::STRING) {
									CHECK(document[f.name.c_str()].GetStringLength() < 2048);
									v.p_val.string_val = std::string(document[f.name.c_str()].GetString());
								} else if (f.type == RowType::DOUBLE) {
									std::string val = std::string(document[f.name.c_str()].GetString());
									try {
										v.p_val.double_val = std::stod(val);
									} catch (std::exception &e) {
										v.p_val.double_val = 0;
									}
								}
                r->write(f, v);
							}
							return r;
						} else {
							LOG(INFO) << "invalid data";
							return nullptr;
						}
			});
      t1->appendRows(messages);
      messages.clear();

      auto t2 = processors::map(t1, schema_ptr, [&](RowBuffer in, RowBuffer out) {
        for (auto f : schema_ptr->fields) {
          Value v;
          in.read(f, v);
          out.write(f, v);
        }
      });
      t1->release();
      auto t3 = processors::shuffle(t2, field3);
      t3->verify(field3);
      processors::reduce(t3, field3, results_ptr, schema_ptr, [=](Value& key,std::vector<std::unique_ptr<RowBuffer>>& vals, std::shared_ptr<RowBuffer>& result){
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
