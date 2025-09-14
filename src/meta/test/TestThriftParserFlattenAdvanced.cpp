/*
 * Additional flatten list/map validation with custom separator and deeper structs
 */
#include <gtest/gtest.h>
#include <fstream>
#include <arrow/api.h>

#include "meta/thrift_parser.h"

using namespace matcha::meta;

static std::string writeTempThrift4(const std::string& content) {
  char path[] = "/tmp/test_thrift_flatten_adv_XXXXXX";
  int fd = mkstemp(path);
  close(fd);
  std::ofstream out(path);
  out << content;
  out.close();
  return std::string(path);
}

TEST(ThriftParserFlattenAdvanced, CustomSepAndDeeperStructs) {
  std::string thrift = R"IDL(
    struct Inner { 1: i64 id, 2: string name }
    struct Middle { 1: Inner inner }
    struct Outer { 1: list<Middle> mids, 2: map<string, Middle> mm }
  )IDL";
  auto path = writeTempThrift4(thrift);
  ThriftParseOptions opt;
  auto schema = ThriftSchemaParser::parseToArrowFlattened(path, "Outer", opt, "__", /*flatten_list_structs*/true, /*flatten_map_structs*/true);

  // Expect mids__inner as list<struct<id:int64, name:utf8>> and mm__inner as map<utf8, struct<...>>
  auto f = [&](const std::string& n){ return schema->GetFieldByName(n); };
  ASSERT_TRUE(f("mids__inner"));
  ASSERT_EQ(f("mids__inner")->type()->id(), arrow::Type::LIST);
  auto mids_list_t = std::static_pointer_cast<arrow::ListType>(f("mids__inner")->type());
  ASSERT_EQ(mids_list_t->value_type()->id(), arrow::Type::STRUCT);
  auto mids_struct_t = std::static_pointer_cast<arrow::StructType>(mids_list_t->value_type());
  ASSERT_EQ(mids_struct_t->num_fields(), 2);
  EXPECT_EQ(mids_struct_t->field(0)->type()->id(), arrow::Type::INT64);
  EXPECT_EQ(mids_struct_t->field(1)->type()->id(), arrow::Type::STRING);

  ASSERT_TRUE(f("mm__inner"));
  ASSERT_EQ(f("mm__inner")->type()->id(), arrow::Type::MAP);
  auto mm_t = std::static_pointer_cast<arrow::MapType>(f("mm__inner")->type());
  EXPECT_EQ(mm_t->key_type()->id(), arrow::Type::STRING);
  ASSERT_EQ(mm_t->item_type()->id(), arrow::Type::STRUCT);
}
