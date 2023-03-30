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

class engine {
public:
  static Declaration source(std::shared_ptr<mtable> t1) {
    auto arrow_t1 = utils::toArrow(t1);
    auto table_1 = arrow::Table::FromRecordBatches({ std::move(arrow_t1) }).ValueOrDie();
    int max_row = t1->row_count;
    CHECK_GT(max_row, 0); //table source needs not be empty
    auto table_source_options = TableSourceNodeOptions(table_1, max_row);
    Declaration source("table_source", std::move(table_source_options));
    return source;
  }

  static Declaration filter(Declaration& node_in, Expression& expression) {
    Declaration filter_plan{
      "filter", { std::move(node_in) }, FilterNodeOptions(std::move(expression))
    };
    return filter_plan;
  }

  static Declaration project(Declaration& node_in, Expression& expression) {
    Declaration project_plan{
      "project", { std::move(node_in) }, ProjectNodeOptions({ expression })
    };
    return project_plan;
  }

  static Declaration union_op(Declaration& left_in, Declaration& right_in) {
    left_in.label = "lhs";
    right_in.label = "rhs";
    Declaration union_plan{
      "union", { std::move(left_in), std::move(right_in) }, ExecNodeOptions{}
    };
    return union_plan;
  }

  static Declaration join(Declaration& left_in, Declaration& right_in, HashJoinNodeOptions& opts, std::string name) {
    Declaration join_plan{
      name, { std::move(left_in), std::move(right_in) }, std::move(opts)
    };
    return join_plan;
  }
};
} // namespace engine
} // namespace surfingdb
#endif // SURFINGDB_NODE_H