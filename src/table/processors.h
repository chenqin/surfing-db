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

#ifndef SURFINGDB_PROCESSORS_H
#define SURFINGDB_PROCESSORS_H

// define static method name in java side
#define BRIDGE_METHOD_NAME "_internal_invoke"

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <iostream>
#include "mtable.h"
#include "xgbop.h"
// #include <torch/csrc/distributed/c10d/ProcessGroupMPI.hpp>
// #include <torch/csrc/distributed/c10d/Work.hpp>
// #include <torch/torch.h>

#pragma once

namespace surfingdb {
namespace table {
class processors {
public:
  /**
   * map convert mtable to new table with schema ptr, if map function return true, otherwise current row
   */
  static std::shared_ptr<mtable> map(std::shared_ptr<mtable>, std::shared_ptr<mschema>, std::function<bool(mrow&, mrow&, const mschema&)>);

  static void reduce(std::shared_ptr<mtable>, Field&, std::shared_ptr<std::unordered_map<Value, std::shared_ptr<mrow>, ValueHasher>> result_ptr, std::shared_ptr<mschema> result_schema_ptr, std::function<void(Value&, std::vector<std::unique_ptr<mrow>>&, std::shared_ptr<mrow>&)>);

  /**
   * @brief shuffle sorted data with one side RMA MPI_Get network call
   *
   * @return std::shared_ptr<mtable>
   */
  static std::shared_ptr<mtable> shuffle_one_side(std::shared_ptr<mtable>, Field&, std::function<size_t(size_t, int, int)>);
  /**
     * @brief shuffle sorted data with two sice isend, irecv network call
     *
     * @return std::shared_ptr<mtable>
     */
    static std::shared_ptr<mtable> shuffle_two_side(std::shared_ptr<mtable>, Field&, std::function<size_t(size_t, int, int)>);
  // arrow format apis
  /**
   * @brief merge recordbatches and do one shuffle
   * 
   * @param node 
   * @return std::shared_ptr<arrow::RecordBatch> 
   */
  const static std::shared_ptr<arrow::RecordBatch> shuffle_x(std::vector<std::shared_ptr<arrow::RecordBatch>>, std::string, std::function<size_t(size_t, int, int)>, bool, std::shared_ptr<node>);

  const static std::vector<std::shared_ptr<arrow::RecordBatch>> java_x(std::vector<std::shared_ptr<arrow::RecordBatch>>, std::string class_name, std::shared_ptr<node> node);

  const static std::shared_ptr<arrow::RecordBatch> java(std::shared_ptr<arrow::RecordBatch>, std::string class_name, std::shared_ptr<node> node);

  /**
   * @brief train a xgboost model
   *
   */
  static void xgb(std::shared_ptr<mtable>, std::vector<Field>, Field&, const XGBParameters&);
};
} // namespace table
} // namespace surfingdb
#endif // SURFINGDB_PROCESSORS_H
