/*
 * Additional tests for mrow read/write on strings, lists, and maps
 */
#include <gtest/gtest.h>

#include "table/mrow.h"
#include "meta/schema.h"

using namespace matcha::table;
using namespace matcha::table::schema;

TEST(MRowTest, WriteReadCompositeTypes) {
  RowSchema r;
  auto fs = SchemaUtils::initField(r, "s", RowType::STRING, 64);
  auto fl = SchemaUtils::initListField(r, "l", RowType::DOUBLE, 2, sizeof(DOUBLE_TYPE));
  auto fm = SchemaUtils::initMapField(r, "m", RowType::STRING, RowType::LONG, 2, 16, sizeof(long));
  auto ms = std::make_shared<mschema>(r);

  mrow row(ms);

  Value vs, vl, vm;
  vs.p_val.string_val = "alpha";
  PValue p1; p1.double_val = 1.5; PValue p2; p2.double_val = 2.5;
  vl.list_value = {p1, p2};
  PValue k1; k1.string_val = "k1"; PValue v1; v1.long_val = 42;
  PValue k2; k2.string_val = "k2"; PValue v2; v2.long_val = 7;
  vm.map_value.insert({k1, v1});
  vm.map_value.insert({k2, v2});

  row.write(fs, vs);
  row.write(fl, vl);
  row.write(fm, vm);

  Value rs, rl, rm;
  row.read(fs, rs);
  row.read(fl, rl);
  row.read(fm, rm);

  EXPECT_EQ(rs.p_val.string_val, vs.p_val.string_val);
  ASSERT_EQ(rl.list_value.size(), 2u);
  EXPECT_FLOAT_EQ(rl.list_value[0].double_val, 1.5);
  EXPECT_FLOAT_EQ(rl.list_value[1].double_val, 2.5);
  ASSERT_EQ(rm.map_value.size(), 2u);
  // validate both keys exist
  bool has_k1 = false, has_k2 = false;
  for (auto &p : rm.map_value) {
    if (p.first.string_val == "k1" && p.second.long_val == 42) has_k1 = true;
    if (p.first.string_val == "k2" && p.second.long_val == 7) has_k2 = true;
  }
  EXPECT_TRUE(has_k1);
  EXPECT_TRUE(has_k2);
}

