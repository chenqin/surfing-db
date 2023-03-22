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
#include <chrono>
#include <fstream>
#include <gtest/gtest.h>
#include "connector/datagen.h"
#include "engine/engine.h"
#include "table/utils.h"

namespace surfingdb {
namespace engine {
namespace test {

namespace cp = ::arrow::compute;
/**
 * https://stackoverflow.com/questions/440133/how-do-i-create-a-random-alpha-numeric-string-in-c
 */
std::string random_string(size_t length) {
  auto randchar = []() -> char {
    const char charset[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = (sizeof(charset) - 1);
    return charset[rand() % max_index];
  };
  std::string str(length, 0);
  std::generate_n(str.begin(), length, randchar);
  return str;
}

arrow::Status ExecutePlanAndCollectAsTable(cp::Declaration plan) {
  // collect sink_reader into a Table
  std::shared_ptr<arrow::Table> response_table;
  ARROW_ASSIGN_OR_RAISE(response_table, DeclarationToTable(std::move(plan)));

  std::cout << "Results : " << response_table->ToString() << std::endl;

  return arrow::Status::OK();
}

TEST(EngineTest, TestEngineSource) {
  // define how data will be stored, as rows in a table
  RowSchema r;
  SchemaUtils::initField(r, "timestamp", RowType::LONG, sizeof(long));
  SchemaUtils::initField(r, "host", RowType::STRING, 64);
  SchemaUtils::initField(r, "metricName", RowType::STRING, MAX_STR_LEN);
  SchemaUtils::initListField(r, "metricValues", RowType::DOUBLE, 2, sizeof(DOUBLE_TYPE));
  SchemaUtils::initMapField(r, "meta", RowType::STRING, RowType::STRING, 1, 32, 64);

  const auto schema_ptr = std::make_shared<mschema>(r);
  /**
   * @brief define a data gen source
   *
   */
  auto deser = [](const char* payload, const mschema& out) {
    auto row = std::make_shared<mrow>(std::make_shared<mschema>(out));
    Value p;

    p.p_val.long_val = 1;
    row->write(out.fields.at(0), p);

    p.p_val.string_val = "hello_host";
    row->write(out.fields.at(1), p);

    p.p_val.string_val = random_string(16);
    row->write(out.fields.at(2), p);

    p.p_val.double_val = 0.1;
    std::vector<PValue> lval;
    lval.push_back(p.p_val);
    lval.push_back(p.p_val);
    row->write(out.fields.at(3), p);
    PValue key;
    PValue value;
    key.string_val = random_string(MAX_STR_LEN - 1);
    value.string_val = random_string(MAX_STR_LEN - 1);
    std::pair<PValue, PValue> pair;
    pair.first = key;
    pair.second = value;
    p.map_value.insert(pair);
    row->write(out.fields.at(4), p);
    return row;
  };
  auto con = DataGenConnector(nullptr, "source", 1, 10, schema_ptr, deser);
  auto source = engine::source(con, "source");
  CHECK_EQ(arrow::Status::OK(), ExecutePlanAndCollectAsTable(std::move(source)));
}

TEST(EngineTest, TestSourceFilter) {
  // define how data will be stored, as rows in a table
  RowSchema r;
  SchemaUtils::initField(r, "timestamp", RowType::LONG, sizeof(long));
  SchemaUtils::initField(r, "host", RowType::STRING, 64);
  SchemaUtils::initField(r, "metricName", RowType::STRING, MAX_STR_LEN);
  SchemaUtils::initListField(r, "metricValues", RowType::DOUBLE, 2, sizeof(DOUBLE_TYPE));
  SchemaUtils::initMapField(r, "meta", RowType::STRING, RowType::STRING, 1, 32, 64);

  const auto schema_ptr = std::make_shared<mschema>(r);
  /**
   * @brief define a data gen source
   *
   */
  auto deser = [](const char* payload, const mschema& out) {
    auto row = std::make_shared<mrow>(std::make_shared<mschema>(out));
    Value p;

    p.p_val.long_val = 1;
    row->write(out.fields.at(0), p);

    p.p_val.string_val = "hello_host";
    row->write(out.fields.at(1), p);

    p.p_val.string_val = random_string(16);
    row->write(out.fields.at(2), p);

    p.p_val.double_val = 0.1;
    std::vector<PValue> lval;
    lval.push_back(p.p_val);
    lval.push_back(p.p_val);
    row->write(out.fields.at(3), p);
    PValue key;
    PValue value;
    key.string_val = random_string(MAX_STR_LEN - 1);
    value.string_val = random_string(MAX_STR_LEN - 1);
    std::pair<PValue, PValue> pair;
    pair.first = key;
    pair.second = value;
    p.map_value.insert(pair);
    row->write(out.fields.at(4), p);
    return row;
  };
  auto con = DataGenConnector(nullptr, "source", 1, 10, schema_ptr, deser);
  auto source = engine::source(con, "source");
  // Expression a_times_2 = cp::call("multiply", {cp::field_ref("timestamp"), cp::literal(2)});
  // auto project = engine::project(source, a_times_2, "multiply");
  Expression filter_expr = cp::less(cp::field_ref("timestamp"), cp::literal(3));
  auto filter = engine::filter(source, filter_expr, "filter");
  CHECK_EQ(arrow::Status::OK(), ExecutePlanAndCollectAsTable(std::move(filter)));
}

TEST(EngineTest, TestSourceUnion) {
  // define how data will be stored, as rows in a table
  RowSchema r;
  SchemaUtils::initField(r, "timestamp", RowType::LONG, sizeof(long));
  SchemaUtils::initField(r, "host", RowType::STRING, 64);
  SchemaUtils::initField(r, "metricName", RowType::STRING, MAX_STR_LEN);
  SchemaUtils::initListField(r, "metricValues", RowType::DOUBLE, 2, sizeof(DOUBLE_TYPE));
  SchemaUtils::initMapField(r, "meta", RowType::STRING, RowType::STRING, 1, 32, 64);

  const auto schema_ptr = std::make_shared<mschema>(r);
  /**
   * @brief define a data gen source
   *
   */
  auto deser = [](const char* payload, const mschema& out) {
    auto row = std::make_shared<mrow>(std::make_shared<mschema>(out));
    Value p;

    p.p_val.long_val = 1;
    row->write(out.fields.at(0), p);

    p.p_val.string_val = "hello_host";
    row->write(out.fields.at(1), p);

    p.p_val.string_val = random_string(16);
    row->write(out.fields.at(2), p);

    p.p_val.double_val = 0.1;
    std::vector<PValue> lval;
    lval.push_back(p.p_val);
    lval.push_back(p.p_val);
    row->write(out.fields.at(3), p);
    PValue key;
    PValue value;
    key.string_val = random_string(MAX_STR_LEN - 1);
    value.string_val = random_string(MAX_STR_LEN - 1);
    std::pair<PValue, PValue> pair;
    pair.first = key;
    pair.second = value;
    p.map_value.insert(pair);
    row->write(out.fields.at(4), p);
    return row;
  };
  auto con = DataGenConnector(nullptr, "source", 1, 10, schema_ptr, deser);
  auto source_left = engine::source(con, "source");
  auto source_right = engine::source(con, "source");
  auto union_plan = engine::union_op(source_left, source_right, "union");
  CHECK_EQ(arrow::Status::OK(), ExecutePlanAndCollectAsTable(std::move(union_plan)));
}

TEST(EngineTest, TestSourceJoin) {
  // define how data will be stored, as rows in a table
  RowSchema r;
  SchemaUtils::initField(r, "timestamp", RowType::LONG, sizeof(long));
  SchemaUtils::initField(r, "host", RowType::STRING, 64);
  SchemaUtils::initField(r, "metricName", RowType::STRING, MAX_STR_LEN);
  /**
   * @brief current join doesn't support composite field
   * 
   */

  const auto schema_ptr = std::make_shared<mschema>(r);
  /**
   * @brief define a data gen source
   *
   */
  auto deser = [](const char* payload, const mschema& out) {
    auto row = std::make_shared<mrow>(std::make_shared<mschema>(out));
    Value p;

    p.p_val.long_val = 1;
    row->write(out.fields.at(0), p);

    p.p_val.string_val = "hello_host";
    row->write(out.fields.at(1), p);

    p.p_val.string_val = random_string(16);
    row->write(out.fields.at(2), p);
    return row;
  };
  auto con = DataGenConnector(nullptr, "source", 1, 10, schema_ptr, deser);
  auto source_left = engine::source(con, "source");
  auto source_right = engine::source(con, "source");
  cp::HashJoinNodeOptions join_opts{

    cp::JoinType::INNER,

    /*left_keys=*/{ "timestamp" },

    /*right_keys=*/{ "timestamp" }, cp::literal(true), "_l", "_r"
  };
  auto join_plan = engine::join(source_left, source_right, join_opts, "hashjoin");
  CHECK_EQ(arrow::Status::OK(), ExecutePlanAndCollectAsTable(std::move(join_plan)));
}

} // namespace test
} // namespace engine
} // namespace surfingdb