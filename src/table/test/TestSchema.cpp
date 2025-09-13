/*
 * Additional tests for schema utilities and mschema sizing
 */
#include <gtest/gtest.h>

#include "table/mrow.h"
#include "table/mtable.h"
#include "meta/schema.h"

using namespace matcha::table;
using namespace matcha::table::schema;

TEST(SchemaTest, RowSizeAndFieldSizes) {
  RowSchema r;
  // primitives
  auto fa = SchemaUtils::initField(r, "a", RowType::INT, sizeof(int));
  auto fb = SchemaUtils::initField(r, "b", RowType::LONG, sizeof(long));
  auto fc = SchemaUtils::initField(r, "c", RowType::BOOL, sizeof(bool));
  auto fd = SchemaUtils::initField(r, "d", RowType::DOUBLE, sizeof(DOUBLE_TYPE));
  auto fe = SchemaUtils::initField(r, "e", RowType::STRING, 16);
  // list of char with max 3, element size = 1
  auto fl = SchemaUtils::initListField(r, "l", RowType::CHAR, 3, sizeof(char));
  // map<string,long> with up to 2 pairs
  auto fm = SchemaUtils::initMapField(r, "m", RowType::STRING, RowType::LONG, 2, 8, sizeof(long));

  mschema ms(r);
  size_t expected = sizeof(size_t); // schema signature stored in row header
  expected += SchemaUtils::getFieldSize(fa);
  expected += SchemaUtils::getFieldSize(fb);
  expected += SchemaUtils::getFieldSize(fc);
  expected += SchemaUtils::getFieldSize(fd);
  expected += SchemaUtils::getFieldSize(fe);
  expected += SchemaUtils::getFieldSize(fl);
  expected += SchemaUtils::getFieldSize(fm);
  EXPECT_EQ(ms.rowSize(), expected);

  // signature stable and non-zero
  EXPECT_NE(ms.signature(), 0u);
}

