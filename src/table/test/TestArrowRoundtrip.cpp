/*
 * Additional tests for Arrow roundtrip conversions and IPC
 */
#include <gtest/gtest.h>

#include "table/utils.h"
#include "table/mtable.h"

using namespace matcha::table;
using namespace matcha::table::schema;

TEST(ArrowRoundtripTest, ToFromArrowAndIpc) {
  RowSchema r;
  auto fa = SchemaUtils::initField(r, "a", RowType::INT, sizeof(int));
  auto fb = SchemaUtils::initField(r, "b", RowType::STRING, 32);
  auto fl = SchemaUtils::initListField(r, "l", RowType::CHAR, 3, sizeof(char));
  auto fm = SchemaUtils::initMapField(r, "m", RowType::STRING, RowType::LONG, 2, 8, sizeof(long));
  auto schema_ptr = std::make_shared<mschema>(r);

  auto table = std::make_shared<mtable>(schema_ptr, schema_ptr->rowSize() * 2);

  // row 1
  mrow r1(schema_ptr);
  Value va, vb, vl, vm; va.p_val.int_val = 10; vb.p_val.string_val = "x";
  PValue e1; e1.byte_val = 'a'; PValue e2; e2.byte_val = 'b';
  vl.list_value = {e1, e2};
  PValue k; k.string_val = "kk"; PValue v; v.long_val = 1l; vm.map_value.insert({k, v});
  r1.write(fa, va); r1.write(fb, vb); r1.write(fl, vl); r1.write(fm, vm);
  table->appendRow(r1);

  // row 2
  mrow r2(schema_ptr);
  va.p_val.int_val = 20; vb.p_val.string_val = "y"; vl.list_value = {e2};
  vm.map_value.clear(); vm.map_value.insert({k, v});
  r2.write(fa, va); r2.write(fb, vb); r2.write(fl, vl); r2.write(fm, vm);
  table->appendRow(r2);

  auto rb = utils::toArrow(table);
  ASSERT_EQ(rb->num_rows(), 2);
  ASSERT_EQ(rb->num_columns(), 4);
  // IPC serialize/deserialize roundtrip
  auto buf = utils::serialize(rb);
  auto rb2 = utils::deserialize(buf, rb->schema());
  ASSERT_EQ(rb2->num_rows(), rb->num_rows());

  // Back to internal representation
  std::map<std::string, uint64_t> units{{"b", 32}, {"l", 3}, {"m", 2}};
  auto table2 = utils::fromArrow({rb2}, units, nullptr);
  ASSERT_EQ(table2->row_size(), table->row_size());

  auto rr0 = table2->readRow(0);
  Value out_b; rr0->read(schema_ptr->getFieldByName("b"), out_b);
  EXPECT_EQ(out_b.p_val.string_val, "x");
}

