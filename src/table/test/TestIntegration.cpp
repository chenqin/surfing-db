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
#include <iostream>
#include <kll_sketch.hpp>
#include <random>
#include "frequent_items_sketch.hpp"
#include "table/mtable.h"

namespace surfingdb {
namespace table {
namespace test {
TEST(TableTest, testCompact) {
  RowSchema r;
  r.fields = std::vector<surfingdb::table::schema::Field>();

  Field field1, field2, field3, field4, field5, field6, field7;

  SchemaUtils::initField(field1, "a", RowType::INT, sizeof(int));
  SchemaUtils::initField(field2, "b", RowType::LONG, sizeof(long));
  SchemaUtils::initField(field3, "c", RowType::BOOL, sizeof(bool));
  SchemaUtils::initField(field4, "d", RowType::DOUBLE, sizeof(DOUBLE_TYPE));
  SchemaUtils::initField(field5, "e", RowType::STRING, 64);

  SchemaUtils::initListField(field6, "l", RowType::DOUBLE, 3, sizeof(DOUBLE_TYPE));
  SchemaUtils::initMapField(field7, "m", RowType::STRING, RowType::LONG, 3, MAX_STR_LEN, sizeof(long));

  r.fields.push_back(field1);
  r.fields.push_back(field2);
  r.fields.push_back(field3);
  r.fields.push_back(field4);
  r.fields.push_back(field5);
  r.fields.push_back(field6);
  r.fields.push_back(field7);
  Value v1, v2, v3, v4, v5, v6, v7;
  v1.p_val.int_val = 3;

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

  // build continuous buffer with fixed fields offsets
  std::shared_ptr<mschema> tpr = std::make_shared<mschema>(r);
  surfingdb::table::mrow b(tpr);
  b.write(field1, v1);
  b.write(field2, v2);
  b.write(field3, v3);
  b.write(field4, v4);
  b.write(field5, v5);
  b.write(field6, v6);
  b.write(field7, v7);
  mtable t(nullptr, tpr, MEM_PAGE_SIZE);
  t.appendRow(b);
  auto compact = t.compactTable();
  EXPECT_LT(compact->getSchema()->rowSize(), tpr->rowSize());

  CHECK(arrow::Status::OK() == t.toColumnar());
  auto q = t.getArrowTable();
  CHECK_EQ(q->num_rows(), 1);
  CHECK_EQ(q->num_columns(), r.fields.size());
}

TEST(TableTest, testmrow) {
  RowSchema r;
  r.fields = std::vector<surfingdb::table::schema::Field>();

  Field field1, field2, field3, field4, field5, field6, field7;

  SchemaUtils::initField(field1, "a", RowType::INT, sizeof(int));
  SchemaUtils::initField(field2, "b", RowType::LONG, sizeof(long));
  SchemaUtils::initField(field3, "c", RowType::BOOL, sizeof(bool));
  SchemaUtils::initField(field4, "d", RowType::DOUBLE, sizeof(DOUBLE_TYPE));
  SchemaUtils::initField(field5, "e", RowType::STRING, MAX_STR_LEN);

  SchemaUtils::initListField(field6, "l", RowType::DOUBLE, 3, sizeof(DOUBLE_TYPE));
  SchemaUtils::initMapField(field7, "m", RowType::STRING, RowType::LONG, 3, MAX_STR_LEN, sizeof(long));

  r.fields.push_back(field1);
  r.fields.push_back(field2);
  r.fields.push_back(field3);
  r.fields.push_back(field4);
  r.fields.push_back(field5);
  r.fields.push_back(field6);
  r.fields.push_back(field7);

  Value v1, v2, v3, v4, v5, v6, v7;
  v1.p_val.int_val = 3;

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

  // build continuous buffer with fixed fields offsets
  std::shared_ptr<mschema> tpr = std::make_shared<mschema>(r);
  surfingdb::table::mrow b(tpr);
  b.write(field1, v1);
  b.write(field2, v2);
  b.write(field3, v3);
  b.write(field4, v4);
  b.write(field5, v5);
  b.write(field6, v6);
  b.write(field7, v7);

  b.read(field1, v1);
  b.read(field2, v2);
  b.read(field3, v3);
  b.read(field4, v4);
  b.read(field5, v6);
  b.read(field6, v5);
  b.read(field7, v3);
  EXPECT_EQ(v6.p_val.string_val, "hello");
  EXPECT_EQ(v5.list_value.size(), 1);
  EXPECT_EQ(v3.map_value.size(), 1);

  // verify read by reference
  Value v11, v22, v33, v44, v55, v66, v77;
  auto s = mrow(tpr, b.payload_ptr());
  s.read(field1, v11);
  s.read(field2, v22);
  s.read(field3, v33);
  s.read(field4, v44);
  s.read(field5, v55);
  s.read(field6, v66);
  s.read(field7, v77);
  EXPECT_EQ(v55.p_val.string_val, "hello");
  EXPECT_EQ(v66.list_value.size(), 1);
  EXPECT_EQ(v77.map_value.size(), 1);

  // test copy to memory and point to memory by ptr
  uint8_t buf[tpr->rowSize()];
  memset(buf, 0, tpr->rowSize());
  memcpy(buf, b.payload_ptr(), tpr->rowSize());

  s = mrow(tpr, &buf[0]);
  s.read(field1, v11);
  s.read(field2, v22);
  s.read(field3, v33);
  s.read(field4, v44);
  s.read(field5, v55);
  s.read(field6, v66);
  s.read(field7, v77);
  EXPECT_EQ(v55.p_val.string_val, "hello");
  EXPECT_EQ(v66.list_value.size(), 1);
  EXPECT_EQ(v77.map_value.size(), 1);

  // test point to temp table
  mtable t(nullptr, tpr, MEM_PAGE_SIZE);
  t.appendRow(s);
  s = mrow(tpr, t.payload_ptr());
  s.read(field1, v11);
  EXPECT_EQ(v11.p_val.int_val, 3);

  auto sptr = t.readRow(0);
  sptr->read(field1, v11);
  sptr->read(field2, v22);
  sptr->read(field3, v33);
  sptr->read(field4, v44);
  sptr->read(field5, v55);
  sptr->read(field6, v66);
  sptr->read(field7, v77);
  EXPECT_EQ(v11.p_val.int_val, 3);
  EXPECT_EQ(v55.p_val.string_val, "hello");
  EXPECT_EQ(v66.list_value.size(), 1);
  EXPECT_EQ(v77.map_value.size(), 1);

  t.appendRow(s);
  sptr = t.readRow(1);
  sptr->read(field1, v77);
  sptr->read(field2, v22);
  sptr->read(field3, v33);
  sptr->read(field4, v44);
  sptr->read(field5, v22);
  sptr->read(field6, v44);
  sptr->read(field7, v33);
  EXPECT_EQ(v77.p_val.int_val, 3);
  EXPECT_EQ(v22.p_val.string_val, "hello");
  EXPECT_EQ(v44.list_value.size(), 1);
  EXPECT_EQ(v33.map_value.size(), 1);

  for (int i = 0; i < 10000; i++) {
    t.appendRow(s);
  }

  // t.flush("/tmp/test1.bin");
  // t.load("/tmp/test1.bin");
  sptr = t.readRow(1);
  Value v;
  sptr->read(field7, v);
  EXPECT_EQ(v.map_value.size(), 1);
  std::shared_ptr<mtable> ptr = t.compactTable();
  EXPECT_EQ(ptr->row_size(), 10002);
  sptr = t.readRow(1);
  Value vm;
  sptr->read(field7, vm);
  EXPECT_EQ(vm.map_value.size(), 1);
}

TEST(TableTest, TestXGBOperator) {
  Field f;
  SchemaUtils::initField(f, "test", RowType::DOUBLE, sizeof(DOUBLE_TYPE));
  std::vector<Field> features;
  features.push_back(f);
  XGBParameters parameters;
  parameters.tree_method = "hist";
  parameters.objective = "binary:logistic";
  parameters.min_child_weight = 1;
  parameters.gamma = 0.1;
  parameters.max_depth = 1;
  parameters.verbosity = true;
  parameters.eval_metric = "error";
  xgbop op(features, f, parameters, 0, 1);
  const float data1[] = { 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0, 0,
                          0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0 };
  const float label1[] = { 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0,
                           0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0 };
  op.train(data1, label1, 50, 1);
  const float label2[] = { 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 1, 0, 0, 1, 0,
                           0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0 };
  op.predict(data1, label2, 50, 1);
}

TEST(TableTest, TestSketchFrequency) {
  typedef datasketches::frequent_items_sketch<std::string> frequent_strings_sketch;

  // this section generates two sketches and serializes them into files
  {
    frequent_strings_sketch sketch1(64);
    sketch1.update("a");
    sketch1.update("a");
    sketch1.update("b");
    sketch1.update("c");
    sketch1.update("a");
    sketch1.update("d");
    sketch1.update("a");
    std::ofstream os1("freq_str_sketch1.bin");
    sketch1.serialize(os1);

    frequent_strings_sketch sketch2(64);
    sketch2.update("e");
    sketch2.update("a");
    sketch2.update("f");
    sketch2.update("f");
    sketch2.update("f");
    sketch2.update("g");
    sketch2.update("a");
    sketch2.update("f");
    std::ofstream os2("freq_str_sketch2.bin");
    sketch2.serialize(os2);
  }

  // this section deserializes the sketches, produces a union and prints the result
  {
    std::ifstream is1("freq_str_sketch1.bin");
    frequent_strings_sketch sketch1 = frequent_strings_sketch::deserialize(is1);

    std::ifstream is2("freq_str_sketch2.bin");
    frequent_strings_sketch sketch2 = frequent_strings_sketch::deserialize(is2);

    // we could merge sketch2 into sketch1 or the other way around
    // this is an example of using a new sketch as a union and keeping the original sketches intact
    frequent_strings_sketch u(64);
    u.merge(sketch1);
    u.merge(sketch2);

    auto items = u.get_frequent_items(datasketches::NO_FALSE_POSITIVES);
    std::cout << "Frequent strings: " << items.size() << std::endl;
    std::cout << "Str\tEst\tLB\tUB" << std::endl;
    for (auto row : items) {
      std::cout << row.get_item() << "\t" << row.get_estimate() << "\t"
                << row.get_lower_bound() << "\t" << row.get_upper_bound() << std::endl;
    }
  }
}

TEST(TableTest, TestSketchQuantile) {
  // this section generates two sketches from random data and serializes them into files
  {
    std::default_random_engine generator(std::chrono::system_clock::now().time_since_epoch().count());
    std::normal_distribution<float> nd(0, 1); // mean=0, stddev=1

    datasketches::kll_sketch<float> sketch1; // default k=200
    for (int i = 0; i < 10000; i++) {
      sketch1.update(nd(generator)); // mean=0, stddev=1
    }
    std::ofstream os1("kll_sketch_float1.bin");
    sketch1.serialize(os1);

    datasketches::kll_sketch<float> sketch2; // default k=200
    // assert(nd != null);
    for (int i = 0; i < 10000; i++) {
      sketch2.update(nd(generator) + 1); // shift the mean for the second sketch
    }
    std::ofstream os2("kll_sketch_float2.bin");
    sketch2.serialize(os2);
  }

  // this section deserializes the sketches, produces a union and prints some results
  {
    std::ifstream is1("kll_sketch_float1.bin");
    auto sketch1 = datasketches::kll_sketch<float>::deserialize(is1);

    std::ifstream is2("kll_sketch_float2.bin");
    auto sketch2 = datasketches::kll_sketch<float>::deserialize(is2);

    // we could merge sketch2 into sketch1 or the other way around
    // this is an example of using a new sketch as a union and keeping the original sketches intact
    datasketches::kll_sketch<float> u; // default k=200
    u.merge(sketch1);
    u.merge(sketch2);

    // Debug output
    // u.to_stream(std::cout);

    std::cout << "Min, Median, Max values" << std::endl;
    const double fractions[3]{ 0, 0.5, 1 };
    auto quantiles = u.get_quantiles(fractions, 3);
    std::cout << quantiles[0] << ", " << quantiles[1] << ", " << quantiles[2] << std::endl;

    std::cout
      << "Probability Histogram: estimated probability mass in 4 bins: (-inf, -2), [-2, 0), [0, 2), [2, +inf)"
      << std::endl;
    const float split_points[]{ -2, 0, 2 };
    const int num_split_points = 3;
    auto pmf = u.get_PMF(split_points, num_split_points);
    std::cout << pmf[0] << ", " << pmf[1] << ", " << pmf[2] << ", " << pmf[3] << std::endl;

    std::cout << "Frequency Histogram: estimated number map original values in the same bins" << std::endl;
    const int num_bins = num_split_points + 1;
    int histogram[num_bins];
    for (int i = 0; i < num_bins; i++) {
      histogram[i] = pmf[i] * u.get_n(); // scale the fractions by the total count of values
    }
    std::cout << histogram[0] << ", " << histogram[1] << ", " << histogram[2] << ", " << histogram[3]
              << std::endl;
  }
}
} // namespace test
} // namespace table
} // namespace surfingdb
