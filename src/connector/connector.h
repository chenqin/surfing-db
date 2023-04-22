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

#ifndef SURFINGDB_CONNECTOR_H
#define SURFINGDB_CONNECTOR_H

#include <string>
#include <vector>
#include "meta/node.h"
#include "meta/schema.h"
#include "table/mrow.h"
#include "table/mtable.h"

namespace surfingdb {
namespace connector {
using namespace surfingdb::meta;
using namespace surfingdb::table;

/**
 * thin kafka client wrapper
 * https://docs.confluent.io/5.5.1/clients/librdkafka/md_CONFIGURATION.html
 */
class Connector {
public:
  std::string connector_name;
  size_t max_batch_size;
  int timeout;
  std::shared_ptr<mschema> schema_ptr;
  Connector(
    const std::shared_ptr<node> node_ptr,
    std::string connector_name,
    size_t max_batch_size,
    int timeout,
    std::shared_ptr<mschema> schema_ptr) {
    this->node_ptr = node_ptr;
    this->connector_name = connector_name;
    this->max_batch_size = max_batch_size;
    this->timeout = timeout;
    this->schema_ptr = schema_ptr;
  }
  Connector() {}

  /**
   * @brief method that poll records into a mtable
   *
   * @param max_batch_size max size of table returned
   * @param timeout max time in milliseconds for each poll
   * @param schema_ptr schema of table returned
   * @param deser function that convert payload binary to a mrow with schema_ptr
   * @return std::shared_ptr<mtable> micro batch table
   */
  virtual std::shared_ptr<mtable> consume_batch(std::function<std::shared_ptr<mrow>(const char* payload, const mschema& schema)> deser) = 0;
  /**
   * @brief pull records as arrow record batch
   * 
   * @param deser 
   * @return std::shared_ptr<arrow::RecordBatch> 
   */
  virtual std::shared_ptr<arrow::RecordBatch> consume_batch(std::function<void(const char* payload, std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders)> deser) = 0;

  /**
   * @brief
   *
   * @param timeout max time used push data out
   * @param output micro batch table
   * @param ser serialize each row in output and push out
   * @return size_t total number of rows published
   */
  size_t produce_batch() {
    return 0;
  }

  void run(volatile std::sig_atomic_t& terminal_signal) {
    while (terminal_signal == 0) {
      if(arrow_deser != nullptr){ 
        auto t = consume_batch(arrow_deser);
        std::unique_lock<std::mutex> lock(record_mutex);
        // wait until record batch contains less than max_batch_size rows
        record_batch_not_full.wait(lock, [this] { 
          size_t total = 0;
          for(auto& batch : record_batch){
            total += batch->num_rows();
          }
          return total < max_batch_size;
        });
        record_batch.push_back(t);
        lock.unlock();
        record_batch_not_empty.notify_one();
      } else {
        throw std::runtime_error("no arrow deserializer set");
      }
    }
  }

  std::vector<std::shared_ptr<arrow::RecordBatch>> poll_once() {
    std::unique_lock<std::mutex> lock(record_mutex);
    /**
     * @brief this assumes no partition should be 0 rows all the time
     * 
     */
    record_batch_not_empty.wait(lock, [this] { return !this->record_batch.empty(); });
    std::vector<std::shared_ptr<arrow::RecordBatch>> ret = std::move(record_batch);
    record_batch.clear();
    CHECK_EQ(record_batch.size(), 0);
    lock.unlock();
    record_batch_not_full.notify_one();
    return ret;
  }
  void setDeser(std::function<void(const char* payload, std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders)> deser) {
    this->arrow_deser = deser;
  }

  void setDeser(std::function<std::shared_ptr<mrow>(const char* payload, const mschema& schema)> deser) {
    this->deser = deser;
  }

protected:
  std::shared_ptr<node> node_ptr;
  std::mutex record_mutex;
  std::condition_variable record_batch_not_full, record_batch_not_empty;
  std::vector<std::shared_ptr<arrow::RecordBatch>> record_batch;
  std::function<std::shared_ptr<mrow>(const char* payload, const mschema& schema)> deser;
  std::function<void(const char* payload, std::vector<std::shared_ptr<arrow::ArrayBuilder>>& builders)> arrow_deser;
};
} // namespace connector

} // namespace surfingdb
#endif // SURFINGDB_CONNECTOR_H