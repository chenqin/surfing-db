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

#ifndef MATCHA_KAFKA_H
#define MATCHA_KAFKA_H

#include <rdkafka.h>

#include <string>
#include <vector>

#include "connector/connector.h"
#include "meta/node.h"
#include "meta/schema.h"
#include "table/mrow.h"
#include "table/mtable.h"

namespace matcha {
namespace connector {
using namespace matcha::meta;
using namespace matcha::table;

/**
 * thin kafka client wrapper
 * https://docs.confluent.io/5.5.1/clients/librdkafka/md_CONFIGURATION.html
 */
class KafkaConnector : public Connector {
 public:
  KafkaConnector(const std::shared_ptr<node>, std::string, size_t, int,
                 std::shared_ptr<mschema>, std::vector<std::string>,
                 std::string, std::string, bool);

  std::shared_ptr<mtable> consume_batch(
      std::function<std::shared_ptr<mrow>(const char* payload,
                                          const mschema& schema)>
          deser);
  std::shared_ptr<mtable> consume_batch();
  std::shared_ptr<arrow::RecordBatch> consume_batch(
      std::function<
          size_t(const void* payload, size_t len,
               std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders)>
          deser);

  ~KafkaConnector();

 private:
  bool assigned;
  rd_kafka_t* rk; /* Consumer instance handle */
  rd_kafka_topic_conf_t* topic_conf;
  rd_kafka_conf_t* conf;     /* Temporary configuration object */
  rd_kafka_resp_err_t err;   /* librdkafka API error code */
  char errstr[512];          /* librdkafka API error reporting buffer */
  std::string serversetpath; /* serverset path */
  const char* groupid;       /* Argument: Consumer group id */
  char* topics;              /* Argument: list of topics to subscribe to */
  int topic_cnt;             /* Number of topics to subscribe to */
  rd_kafka_topic_partition_list_t* subscription; /* Subscribed topics */
  int i;
  static void print_partition_list(
      const rd_kafka_topic_partition_list_t* partitions, int rank, int world);
  static void cb(rd_kafka_t* rk, rd_kafka_resp_err_t err,
      rd_kafka_topic_partition_list_t* partitions, void* opaque);
  void generate(bool pii);
};
}  // namespace connector

}  // namespace matcha
#endif  //  MATCHA_KAFKA_H
