/*
 * Comprehensive nested Thrift parser tests
 */
#include <gtest/gtest.h>
#include <fstream>
#include <arrow/api.h>

#include "meta/thrift_parser.h"

using namespace matcha::meta;

static std::string writeTempThrift3(const std::string& content) {
  char path[] = "/tmp/test_schema_nested_comp_XXXXXX";
  int fd = mkstemp(path);
  close(fd);
  std::ofstream out(path);
  out << content;
  out.close();
  return std::string(path);
}

TEST(ThriftParserNestedComprehensive, DeepNestedStructs_Arrow) {
  // 3-level nesting with lists and maps under second-level
  std::string thrift = R"IDL(
    // Comments and spacing
    struct Inner { 1: optional bool ok, 2: i64 id, 3: string note; }
    struct Middle {
      1: Inner inner2, // nested struct
      2: list<i16> nums, # list of primitive
      3: map<string, i32> scores, /* map of primitive key/value */
    }
    struct Outer {
      1: required Middle mid; // direct nested
      2: list<Middle> mids;   // list of struct
      3: map<string, Middle> midmap; // map with struct value
      4: double dval; // maps to float32 in Arrow mapping here
    }
  )IDL";
  auto path = writeTempThrift3(thrift);
  ThriftParseOptions opt;
  auto as = ThriftSchemaParser::parseToArrow(path, "Outer", opt);

  ASSERT_EQ(as->num_fields(), 4);

  // Field 0: mid is a struct with inner2 (struct), nums (list<int32>), scores (map<utf8,int32>)
  auto f_mid = as->field(0);
  ASSERT_EQ(f_mid->type()->id(), arrow::Type::STRUCT);
  auto mid_t = std::static_pointer_cast<arrow::StructType>(f_mid->type());
  ASSERT_EQ(mid_t->num_fields(), 3);
  // inner2 struct
  auto mid_inner2 = mid_t->field(0);
  ASSERT_EQ(mid_inner2->type()->id(), arrow::Type::STRUCT);
  auto inner2_t = std::static_pointer_cast<arrow::StructType>(mid_inner2->type());
  ASSERT_EQ(inner2_t->num_fields(), 3);
  EXPECT_EQ(inner2_t->field(0)->type()->id(), arrow::Type::BOOL);
  EXPECT_EQ(inner2_t->field(1)->type()->id(), arrow::Type::INT64);
  EXPECT_EQ(inner2_t->field(2)->type()->id(), arrow::Type::STRING);
  // nums list<i16> mapped to int32
  auto mid_nums = mid_t->field(1);
  ASSERT_EQ(mid_nums->type()->id(), arrow::Type::LIST);
  auto nums_t = std::static_pointer_cast<arrow::ListType>(mid_nums->type());
  EXPECT_EQ(nums_t->value_type()->id(), arrow::Type::INT32);
  // scores map<string,i32>
  auto mid_scores = mid_t->field(2);
  ASSERT_EQ(mid_scores->type()->id(), arrow::Type::MAP);
  auto scores_t = std::static_pointer_cast<arrow::MapType>(mid_scores->type());
  EXPECT_EQ(scores_t->key_type()->id(), arrow::Type::STRING);
  EXPECT_EQ(scores_t->item_type()->id(), arrow::Type::INT32);

  // Field 1: mids is list<struct Middle>
  auto f_mids = as->field(1);
  ASSERT_EQ(f_mids->type()->id(), arrow::Type::LIST);
  auto mids_list_t = std::static_pointer_cast<arrow::ListType>(f_mids->type());
  ASSERT_EQ(mids_list_t->value_type()->id(), arrow::Type::STRUCT);

  // Field 2: midmap is map<utf8, struct Middle>
  auto f_midmap = as->field(2);
  ASSERT_EQ(f_midmap->type()->id(), arrow::Type::MAP);
  auto midmap_t = std::static_pointer_cast<arrow::MapType>(f_midmap->type());
  EXPECT_EQ(midmap_t->key_type()->id(), arrow::Type::STRING);
  ASSERT_EQ(midmap_t->item_type()->id(), arrow::Type::STRUCT);

  // Field 3: dval maps to float32
  auto f_dval = as->field(3);
  EXPECT_EQ(f_dval->type()->id(), arrow::Type::FLOAT);
}

TEST(ThriftParserNestedComprehensive, ArrowFlatten_DefaultAndOptions) {
  std::string thrift = R"IDL(
    struct Inner { 1: i64 id, 2: string name }
    struct Middle { 1: Inner inner, 2: list<Inner> inners, 3: map<string, Inner> m }
    struct Outer { 1: Middle mid, 2: list<Middle> mids, 3: map<string, Middle> mm }
  )IDL";
  auto path = writeTempThrift3(thrift);
  ThriftParseOptions opt;

  // Default flatten: only direct struct fields on Outer are flattened; list/map remain collections of struct
  auto s_def = ThriftSchemaParser::parseToArrowFlattened(path, "Outer", opt);
  // Expect fields: mid_inner_id, mid_inner_name, mid_inners, mid_m, mids, mm
  ASSERT_TRUE(s_def->GetFieldByName("mid_inner_id"));
  ASSERT_TRUE(s_def->GetFieldByName("mid_inner_name"));
  // mids and mm should remain collection-of-struct
  auto f_mids = s_def->GetFieldByName("mids");
  ASSERT_TRUE(f_mids);
  ASSERT_EQ(f_mids->type()->id(), arrow::Type::LIST);
  auto mids_list_t = std::static_pointer_cast<arrow::ListType>(f_mids->type());
  ASSERT_EQ(mids_list_t->value_type()->id(), arrow::Type::STRUCT);
  auto f_mm = s_def->GetFieldByName("mm");
  ASSERT_TRUE(f_mm);
  ASSERT_EQ(f_mm->type()->id(), arrow::Type::MAP);
  auto mm_t = std::static_pointer_cast<arrow::MapType>(f_mm->type());
  ASSERT_EQ(mm_t->item_type()->id(), arrow::Type::STRUCT);

  // Note: list/map-of-struct flatten options are validated in dedicated flatten tests.
}

TEST(ThriftParserNestedComprehensive, MSchema_FlattensNestedStructs) {
  // Only primitives/list<prim>/map<prim,prim> are supported; nested struct fields are flattened with prefixes
  std::string thrift = R"IDL(
    struct Inner { 1: i64 id, 2: string tag }
    struct Middle { 1: Inner inner, 2: i32 n, 3: list<i64> ints, 4: map<string, i32> weights }
    struct Outer { 1: Middle mid }
  )IDL";
  auto path = writeTempThrift3(thrift);
  ThriftParseOptions opt;
  auto ms = ThriftSchemaParser::parseToMSchema(path, "Outer", opt);

  // Expect fields: mid_inner_id, mid_inner_tag, mid_n, mid_ints, mid_weights
  ASSERT_EQ(ms->fields.size(), 5);
  auto has = [&](const std::string& n){
    for (auto& f : ms->fields) if (f.name == n) return true; return false;
  };
  EXPECT_TRUE(has("mid_inner_id"));
  EXPECT_TRUE(has("mid_inner_tag"));
  EXPECT_TRUE(has("mid_n"));
  EXPECT_TRUE(has("mid_ints"));
  EXPECT_TRUE(has("mid_weights"));
}

TEST(ThriftParserNestedComprehensive, MSchema_UnsupportedCollectionsThrow) {
  std::string thrift = R"IDL(
    struct Inner { 1: i64 id }
    struct Outer { 1: list<Inner> inners, 2: map<string, Inner> m }
  )IDL";
  auto path = writeTempThrift3(thrift);
  ThriftParseOptions opt;
  // list<struct> unsupported
  EXPECT_THROW({ ThriftSchemaParser::parseToMSchema(path, "Outer", opt); }, std::runtime_error);
}

TEST(ThriftParserNestedComprehensive, Errors_UnknownStructsAndMapKey) {
  // Unknown top-level struct
  std::string thrift1 = R"IDL(struct A { 1: i32 x })IDL";
  auto p1 = writeTempThrift3(thrift1);
  ThriftParseOptions opt;
  EXPECT_THROW({ ThriftSchemaParser::parseToArrow(p1, "NonExistent", opt); }, std::runtime_error);

  // Unknown nested struct type reference
  std::string thrift2 = R"IDL(struct Outer { 1: UnknownType u })IDL";
  auto p2 = writeTempThrift3(thrift2);
  EXPECT_THROW({ ThriftSchemaParser::parseToArrow(p2, "Outer", opt); }, std::runtime_error);

  // Map with non-primitive key should error in Arrow conversion
  std::string thrift3 = R"IDL(struct K { 1: i32 a } struct Outer { 1: map<K, string> bad })IDL";
  auto p3 = writeTempThrift3(thrift3);
  EXPECT_THROW({ ThriftSchemaParser::parseToArrow(p3, "Outer", opt); }, std::runtime_error);
}

TEST(ThriftParserNestedComprehensive, QualifiersCommentsAndTypeMapping) {
  // Mix required/optional, comments (#, //, /* */), semicolons/commas, and case-insensitive primitive tokens
  std::string thrift = R"IDL(
    struct Mixed {
      1: required BOOL b; // bool
      2: optional I8 c,    # byte
      3: I16 s;            /* i16 -> int32 */
      4: i32 i,
      5: I64 l;
      6: DOUBLE d;         // double -> float32
      7: string str;
    }
  )IDL";
  auto path = writeTempThrift3(thrift);
  ThriftParseOptions opt;
  auto as = ThriftSchemaParser::parseToArrow(path, "Mixed", opt);
  ASSERT_EQ(as->num_fields(), 7);
  EXPECT_EQ(as->field(0)->type()->id(), arrow::Type::BOOL);
  EXPECT_EQ(as->field(1)->type()->id(), arrow::Type::INT8);
  EXPECT_EQ(as->field(2)->type()->id(), arrow::Type::INT32);
  EXPECT_EQ(as->field(3)->type()->id(), arrow::Type::INT32);
  EXPECT_EQ(as->field(4)->type()->id(), arrow::Type::INT64);
  EXPECT_EQ(as->field(5)->type()->id(), arrow::Type::FLOAT); // mapped to float32
  EXPECT_EQ(as->field(6)->type()->id(), arrow::Type::STRING);
}
