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

#ifndef MATCHA_PROCESSORS_H
#define MATCHA_PROCESSORS_H

// define static method name in java side
#define BRIDGE_METHOD_NAME "_internal_invoke"

#include <arrow/api.h>
#include <arrow/compute/api.h>

#include <iostream>
#include <utility>

#include "mtable.h"
#include "xgbop.h"
// #include <torch/csrc/distributed/c10d/ProcessGroupMPI.hpp>
// #include <torch/csrc/distributed/c10d/Work.hpp>
// #include <torch/torch.h>

#pragma once

namespace matcha {
namespace table {
class processors {
 private:
  static std::shared_ptr<arrow::RecordBatch> shuffle_one_side(
      const arrow::RecordBatchVector&, std::string,
      std::function<size_t(size_t, int, int)>, int, int);
  static std::shared_ptr<arrow::RecordBatch> shuffle_two_side(
      const arrow::RecordBatchVector&, std::string,
      std::function<size_t(size_t, int, int)>, int, int);
  // TODO: bug fix
  static std::shared_ptr<arrow::RecordBatch> java(
      const std::shared_ptr<arrow::RecordBatch>&, std::string class_name,
      JNIEnv* env, jclass* clz, jobject* instance);
  static std::shared_ptr<arrow::RecordBatch> java(
      const std::shared_ptr<arrow::RecordBatch>& batch, std::string class_name,
      JNIEnv* env);

 public:
  /**
   * @brief v1 apis
   *
   * @return std::shared_ptr<mtable>
   */
  static std::shared_ptr<mtable> map(
      std::shared_ptr<mtable>, std::shared_ptr<mschema>,
      std::function<bool(mrow&, mrow&, const mschema&)>);
  static void reduce(
      std::shared_ptr<mtable>, Field&,
      std::shared_ptr<
          std::unordered_map<Value, std::shared_ptr<mrow>, ValueHasher>>
          result_ptr,
      std::shared_ptr<mschema> result_schema_ptr,
      std::function<void(Value&, std::vector<std::unique_ptr<mrow>>&,
                         std::shared_ptr<mrow>&)>);
  static void xgb(std::shared_ptr<mtable>, std::vector<Field>, Field&,
                  const XGBParameters&);

  static std::shared_ptr<arrow::RecordBatch> shuffle(
      std::shared_ptr<arrow::RecordBatch>&, std::string,
      std::function<size_t(size_t, int, int)>, bool, int, int);
  // Shuffle both left and right inputs by the same key to the same ranks and
  // return the locally owned partitions. When singleside is true, uses the
  // one-sided MPI window path; otherwise uses the two-sided send/recv path.
  static std::pair<std::shared_ptr<arrow::RecordBatch>,
                   std::shared_ptr<arrow::RecordBatch>>
  cogroup(const arrow::RecordBatchVector& left,
          const arrow::RecordBatchVector& right,
          std::string field_name,
          std::function<size_t(size_t, int, int)> partitioner,
          bool singleside,
          int rank,
          int world);

  // Convenience overload taking single batches
  static std::pair<std::shared_ptr<arrow::RecordBatch>,
                   std::shared_ptr<arrow::RecordBatch>>
  cogroup(std::shared_ptr<arrow::RecordBatch>& left,
          std::shared_ptr<arrow::RecordBatch>& right,
          std::string field_name,
          std::function<size_t(size_t, int, int)> partitioner,
          bool singleside,
          int rank,
          int world);
  static arrow::RecordBatchVector jni(
      const arrow::RecordBatchVector&,
      std::string class_name, JNIEnv* env, int rank);
};
}  // namespace table
}  // namespace matcha
#endif  //  MATCHA_PROCESSORS_H
