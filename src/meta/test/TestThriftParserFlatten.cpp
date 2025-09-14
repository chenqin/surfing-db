/*
 * Tests for ThriftSchemaParser flattening of list/map of struct into multiple fields
 */
#include <gtest/gtest.h>
#include <fstream>
#include <arrow/api.h>

#include "meta/thrift_parser.h"

using namespace matcha::meta;

static std::string writeTempFile(const std::string& content) {
  char path[] = "/tmp/test_thrift_flatten_XXXXXX";
  int fd = mkstemp(path);
  close(fd);
  std::ofstream out(path);
  out << content;
  out.close();
  return std::string(path);
}

TEST(ThriftParserFlattenTest, FlattenListAndMapStructs) {
  std::string thrift = R"IDL(
    struct Inner { 1: i64 id, 2: string name }
    struct Outer { 1: list<Inner> inners, 2: map<string, Inner> m, 3: Inner direct }
  )IDL";
  auto path = writeTempFile(thrift);
  ThriftParseOptions opt;
  auto schema = ThriftSchemaParser::parseToArrowFlattened(path, "Outer", opt, "_", true, true);

  // Expect fields: inners_id (list<int64>), inners_name (list<utf8>), m_id (map<utf8,int64>), m_name (map<utf8,utf8>), direct_id (int64), direct_name (utf8)
  ASSERT_EQ(schema->num_fields(), 6);

  auto f = [&](const std::string& n){ return schema->GetFieldByName(n); };
  ASSERT_TRUE(f("inners_id"));
  ASSERT_EQ(f("inners_id")->type()->id(), arrow::Type::LIST);
  auto l_id = std::static_pointer_cast<arrow::ListType>(f("inners_id")->type());
  EXPECT_EQ(l_id->value_type()->id(), arrow::Type::INT64);

  ASSERT_TRUE(f("inners_name"));
  ASSERT_EQ(f("inners_name")->type()->id(), arrow::Type::LIST);
  auto l_nm = std::static_pointer_cast<arrow::ListType>(f("inners_name")->type());
  EXPECT_EQ(l_nm->value_type()->id(), arrow::Type::STRING);

  ASSERT_TRUE(f("m_id"));
  ASSERT_EQ(f("m_id")->type()->id(), arrow::Type::MAP);
  auto m_id = std::static_pointer_cast<arrow::MapType>(f("m_id")->type());
  EXPECT_EQ(m_id->key_type()->id(), arrow::Type::STRING);
  EXPECT_EQ(m_id->item_type()->id(), arrow::Type::INT64);

  ASSERT_TRUE(f("m_name"));
  ASSERT_EQ(f("m_name")->type()->id(), arrow::Type::MAP);
  auto m_nm = std::static_pointer_cast<arrow::MapType>(f("m_name")->type());
  EXPECT_EQ(m_nm->key_type()->id(), arrow::Type::STRING);
  EXPECT_EQ(m_nm->item_type()->id(), arrow::Type::STRING);

  ASSERT_TRUE(f("direct_id"));
  EXPECT_EQ(f("direct_id")->type()->id(), arrow::Type::INT64);
  ASSERT_TRUE(f("direct_name"));
  EXPECT_EQ(f("direct_name")->type()->id(), arrow::Type::STRING);
}

