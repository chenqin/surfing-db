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
    auto units = utils::toUnits({ arrow_t1 });
    return source({arrow_t1}, units);
  }

  static Declaration source(std::shared_ptr<arrow::RecordBatch> arrow_t1) {
    auto units = utils::toUnits({ arrow_t1 });
    return source({arrow_t1}, units);
  }

  static Declaration source(std::vector<std::shared_ptr<arrow::RecordBatch>> records, std::map<std::string, uint64_t> units) {
    CHECK(records.size() > 0);
    int max_row = 0;
    for (auto& arrow_t1 : records) {
      if (arrow_t1->num_rows() == 0) {
        arrow_t1 = utils::placeholder(arrow_t1->schema(), units);
      }
      max_row += arrow_t1->num_rows();
    }
    CHECK_GT(max_row, 0); // table source needs not be empty
    auto table_1 = arrow::Table::FromRecordBatches(records).ValueOrDie();
    auto table_source_options = TableSourceNodeOptions(table_1, max_row);
    Declaration source("table_source", std::move(table_source_options));
    return source;
  }

  static Declaration shuffle(Declaration& node_in, std::shared_ptr<node> node) {
    auto tables = DeclarationToBatches(std::move(node_in)).ValueOrDie();
    CHECK(tables.size() > 0);

    auto start = MPI_Wtime();
    auto units = utils::toUnits(tables);
    auto mtable = utils::fromArrow(tables, units, node, [](size_t key, int rank, int world) { return key % world; });
    auto schema = utils::fromArrow(tables.at(0)->schema(), units);
    auto t5 = processors::shuffle(mtable, schema->fields.at(0), [](size_t key, int rank, int world) {
      return key % world;
    }, true);
    t5->verifyShuffle(schema->fields.at(0), [](size_t key, int rank, int world) {return key % world;});
    auto t = utils::toArrow(t5);

    LOG(INFO) << "shuffle qps" << t5->row_count / (MPI_Wtime() - start);
    return source({t}, units);
  }

  static Declaration java(Declaration& node_in, std::string classname, std::shared_ptr<node> node) {
    auto tables = DeclarationToBatches(std::move(node_in)).ValueOrDie();
    std::vector<std::shared_ptr<arrow::RecordBatch>> atables;
    auto units = utils::toUnits(tables);
    auto start = MPI_Wtime();
    double total_count = 0;
    for (const auto& t : tables) {
      auto t3 = processors::java(t, classname, node);
      total_count += t3->num_rows();
      atables.push_back(t3);
    }
    LOG(INFO) << "java qps" << total_count / (MPI_Wtime() - start);
    units = utils::toUnits(atables);
    return source(atables, units);
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