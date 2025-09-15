/*
 * Additional coverage for ThriftSchemaParser
 */
#include <gtest/gtest.h>
#include <fstream>
#include <arrow/api.h>

#include "meta/thrift_parser.h"

using namespace matcha::meta;

static std::string writeTempThrift_more(const std::string& content) {
  char path[] = "/tmp/test_thrift_more_XXXXXX";
  int fd = mkstemp(path);
  close(fd);
  std::ofstream out(path);
  out << content;
  out.close();
  return std::string(path);
}

TEST(ThriftParserMore, PrimitiveAliasesFloatAndBinary) {
  std::string thrift = R"IDL(
    struct Aliases { 1: float f; 2: binary b; 3: long l; 4: byte c; 5: int i; }
  )IDL";
  auto path = writeTempThrift_more(thrift);
  ThriftParseOptions opt;
  auto as = ThriftSchemaParser::parseToArrow(path, "Aliases", opt);
  ASSERT_EQ(as->num_fields(), 5);
  EXPECT_EQ(as->field(0)->type()->id(), arrow::Type::FLOAT);  // float -> float32
  EXPECT_EQ(as->field(1)->type()->id(), arrow::Type::STRING); // binary -> utf8
  EXPECT_EQ(as->field(2)->type()->id(), arrow::Type::INT64);  // long -> int64
  EXPECT_EQ(as->field(3)->type()->id(), arrow::Type::INT8);   // byte -> int8
  EXPECT_EQ(as->field(4)->type()->id(), arrow::Type::INT32);  // int -> int32
}

TEST(ThriftParserMore, NestedListsAndMapOfList) {
  std::string thrift = R"IDL(
    struct Outer {
      1: list<list<i32>> ll;
      2: map<string, list<i64>> mlist;
    }
  )IDL";
  auto path = writeTempThrift_more(thrift);
  ThriftParseOptions opt;
  auto as = ThriftSchemaParser::parseToArrow(path, "Outer", opt);
  ASSERT_EQ(as->num_fields(), 2);
  // ll is list<list<int32>>
  auto f_ll = as->field(0);
  ASSERT_EQ(f_ll->type()->id(), arrow::Type::LIST);
  auto ll_t = std::static_pointer_cast<arrow::ListType>(f_ll->type());
  ASSERT_EQ(ll_t->value_type()->id(), arrow::Type::LIST);
  auto inner_list = std::static_pointer_cast<arrow::ListType>(ll_t->value_type());
  EXPECT_EQ(inner_list->value_type()->id(), arrow::Type::INT32);

  // mlist is map<utf8, list<int64>>
  auto f_mlist = as->field(1);
  ASSERT_EQ(f_mlist->type()->id(), arrow::Type::MAP);
  auto m_t = std::static_pointer_cast<arrow::MapType>(f_mlist->type());
  EXPECT_EQ(m_t->key_type()->id(), arrow::Type::STRING);
  ASSERT_EQ(m_t->item_type()->id(), arrow::Type::LIST);
  auto mv_list = std::static_pointer_cast<arrow::ListType>(m_t->item_type());
  EXPECT_EQ(mv_list->value_type()->id(), arrow::Type::INT64);
}

TEST(ThriftParserMore, ParseEmptyStruct) {
  std::string thrift = R"IDL(struct Empty { })IDL";
  auto path = writeTempThrift_more(thrift);
  ThriftParseOptions opt;
  auto as = ThriftSchemaParser::parseToArrow(path, "Empty", opt);
  EXPECT_EQ(as->num_fields(), 0);
}

TEST(ThriftParserMore, BlockCommentsSpanningLines) {
  std::string thrift = R"IDL(
    struct WithComments {
      /* start
         multi-line comment */
      1: i64 a,
      /* another
         comment */ 2: string b; // end line comment
    }
  )IDL";
  auto path = writeTempThrift_more(thrift);
  ThriftParseOptions opt;
  auto as = ThriftSchemaParser::parseToArrow(path, "WithComments", opt);
  ASSERT_EQ(as->num_fields(), 2);
  EXPECT_EQ(as->field(0)->type()->id(), arrow::Type::INT64);
  EXPECT_EQ(as->field(1)->type()->id(), arrow::Type::STRING);
}

TEST(ThriftParserMore, FlattenOptionsPartial) {
  std::string thrift = R"IDL(
    struct Inner { 1: i64 id, 2: string name }
    struct Outer { 1: list<Inner> inners, 2: map<string, Inner> m }
  )IDL";
  auto path = writeTempThrift_more(thrift);
  ThriftParseOptions opt;
  // Flatten lists only
  auto s1 = ThriftSchemaParser::parseToArrowFlattened(path, "Outer", opt, "-", /*flatten_list_structs*/true, /*flatten_map_structs*/false);
  // Expect inners-id and inners-name as list<prim>, but m remains map<utf8, struct>
  auto f_inners_id = s1->GetFieldByName("inners-id");
  auto f_inners_name = s1->GetFieldByName("inners-name");
  ASSERT_TRUE(f_inners_id && f_inners_name);
  ASSERT_EQ(f_inners_id->type()->id(), arrow::Type::LIST);
  ASSERT_EQ(f_inners_name->type()->id(), arrow::Type::LIST);
  auto m_field = s1->GetFieldByName("m");
  ASSERT_TRUE(m_field);
  ASSERT_EQ(m_field->type()->id(), arrow::Type::MAP);
  auto m_t = std::static_pointer_cast<arrow::MapType>(m_field->type());
  ASSERT_EQ(m_t->item_type()->id(), arrow::Type::STRUCT);

  // Flatten maps only
  auto s2 = ThriftSchemaParser::parseToArrowFlattened(path, "Outer", opt, "-", /*flatten_list_structs*/false, /*flatten_map_structs*/true);
  // Expect m-id and m-name as map<utf8, prim>, but inners remains list<struct>
  auto f_m_id = s2->GetFieldByName("m-id");
  auto f_m_name = s2->GetFieldByName("m-name");
  ASSERT_TRUE(f_m_id && f_m_name);
  ASSERT_EQ(f_m_id->type()->id(), arrow::Type::MAP);
  ASSERT_EQ(f_m_name->type()->id(), arrow::Type::MAP);
  auto inners_field = s2->GetFieldByName("inners");
  ASSERT_TRUE(inners_field);
  ASSERT_EQ(inners_field->type()->id(), arrow::Type::LIST);
  auto inn_list = std::static_pointer_cast<arrow::ListType>(inners_field->type());
  ASSERT_EQ(inn_list->value_type()->id(), arrow::Type::STRUCT);
}

TEST(ThriftParserMore, MSchemaRejectsNestedListAndUnknownStruct) {
  // Nested list should be rejected by mschema
  std::string thrift1 = R"IDL(struct Outer { 1: list<list<i32>> bad })IDL";
  auto p1 = writeTempThrift_more(thrift1);
  ThriftParseOptions opt;
  EXPECT_THROW({ ThriftSchemaParser::parseToMSchema(p1, "Outer", opt); }, std::runtime_error);

  // Unknown nested struct reference inside mschema flatten should error
  std::string thrift2 = R"IDL(struct Outer { 1: Unknown u })IDL";
  auto p2 = writeTempThrift_more(thrift2);
  EXPECT_THROW({ ThriftSchemaParser::parseToMSchema(p2, "Outer", opt); }, std::runtime_error);
}

