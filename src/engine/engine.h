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
#include <arrow/array.h>
#include <arrow/builder.h>

#include <arrow/compute/api.h>
#include <arrow/compute/api_vector.h>
#include <arrow/compute/cast.h>
#include <arrow/compute/exec/exec_plan.h>

#include <arrow/csv/api.h>

#include <arrow/dataset/dataset.h>
#include <arrow/dataset/file_base.h>
#include <arrow/dataset/file_parquet.h>
#include <arrow/dataset/plan.h>
#include <arrow/dataset/scanner.h>

#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>

#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/table.h>

#include <arrow/ipc/api.h>

#include <arrow/util/future.h>
#include <arrow/util/range.h>
#include <arrow/util/thread_pool.h>
#include <arrow/util/vector.h>

#include <iostream>
#include <memory>
#include <utility>
#include "connector/connector.h"
#include "table/processors.h"
#include "table/utils.h"

#ifndef SURFINGDB_ENGINE_H
#define SURFINGDB_ENGINE_H
namespace surfingdb {
namespace engine {

using namespace surfingdb::connector;
using namespace arrow::compute;

struct BatchesWithSchema {
  std::vector<arrow::compute::ExecBatch> batches;
  std::shared_ptr<arrow::Schema> schema;
  // This method uses internal arrow utilities to
  // convert a vector of record batches to an AsyncGenerator of optional batches
  arrow::AsyncGenerator<std::optional<arrow::compute::ExecBatch>> gen() const {
    auto opt_batches = ::arrow::internal::MapVector(
      [](arrow::compute::ExecBatch batch) { return std::make_optional(std::move(batch)); },
      batches);
    arrow::AsyncGenerator<std::optional<arrow::compute::ExecBatch>> gen;
    gen = arrow::MakeVectorGenerator(std::move(opt_batches));
    return gen;
  }
};

class engine {
public:
  static Declaration source(Connector& con, std::string name) {
    BatchesWithSchema out;
    auto res_batch = surfingdb::table::utils::toArrow(con.consume_batch());
    arrow::compute::ExecBatch batch{ *res_batch };
    out.batches = { batch };
    out.schema = surfingdb::table::utils::toArrow(con.schema_ptr);

    auto source_node_options = SourceNodeOptions{ out.schema, out.gen() };
    Declaration source{ name, std::move(source_node_options) };

    return source;
  }

  static Declaration filter(Declaration& node_in, Expression& expression, std::string name) {
    Declaration filter_plan{
      name, { std::move(node_in) }, FilterNodeOptions(std::move(expression))
    };
    return filter_plan;
  }

  static Declaration project(Declaration& node_in, Expression& expression, std::string name) {
    Declaration project_plan{
      name, { std::move(node_in) }, ProjectNodeOptions({ expression })
    };
    return project_plan;
  }

  static Declaration union_op(Declaration& left_in, Declaration& right_in, std::string name) {
    left_in.label = "lhs";
    right_in.label = "rhs";
    Declaration union_plan{
      name, { std::move(left_in), std::move(right_in) }, ExecNodeOptions{}
    };
    return union_plan;
  }

  static Declaration join(Declaration& left_in, Declaration& right_in, HashJoinNodeOptions& opts, std::string name) {
    Declaration join_plan{
      name, { std::move(left_in), std::move(right_in) }, std::move(opts)
    };
    return join_plan;
  }
  /**
   * @brief allow data shuffle by field name, this imply break operation chain and rebuild source
   * 
   * @param left_in 
   * @param name 
   * @return Declaration 
   */
  static Declaration partition(Declaration& left_in, std::string name) {
    auto batch_in = DeclarationToBatches(std::move(left_in)).ValueOrDie();
    auto batch = batch_in.at(0);
    std::map<std::string, uint64_t> units;
    std::shared_ptr<mtable> t = utils::fromArrow(batch, units, nullptr);
    std::shared_ptr<mschema> s = utils::fromArrow(batch->schema(), units);
    Field f = s->getFieldByName(name);
    auto post_shuffle_batch = processors::shuffle(t, f, [](size_t key, int rank, int world) {
      return key%world;
    });
    BatchesWithSchema out;
    auto res_batch = surfingdb::table::utils::toArrow(post_shuffle_batch);
    arrow::compute::ExecBatch batch_out{ *res_batch };
    out.batches = { batch_out };
    out.schema = batch->schema();
    auto source_node_options = SourceNodeOptions{ out.schema, out.gen() };
    Declaration source{ "source", std::move(source_node_options) };
  }
};
} // namespace engine
} // namespace surfingdb
#endif // SURFINGDB_NODE_H