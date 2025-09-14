#include <gtest/gtest.h>
#include <fstream>
#include <arrow/api.h>

#include "meta/thrift_parser.h"

using namespace matcha::meta;

static std::string writeTempThrift(const std::string& content) {
  char path[] = "/tmp/test_schema_XXXXXX";
  int fd = mkstemp(path);
  close(fd);
  std::ofstream out(path);
  out << content;
  out.close();
  return std::string(path);
}

TEST(ThriftParserTest, ParseOuterWithMapAndList) {
  std::string thrift = R"IDL(
    namespace cpp test
    // included file (ignored by parser but should not break)
    include "common.thrift"

    struct Inner {
      1: optional i64 id,
      2: optional string note,
    }

    struct Outer {
      1: required map<string, i64> counts,
      2: required list<string> tags,
      3: optional double score,
    }
  )IDL";

  auto path = writeTempThrift(thrift);
  ThriftParseOptions opt;
  opt.default_string_max = 128;
  opt.default_list_len = 16;
  opt.default_map_pairs = 8;
  auto ms = ThriftSchemaParser::parseToMSchema(path, "Outer", opt);
  ASSERT_EQ(ms->fields.size(), 3);
  auto& f_counts_ms = ms->getFieldByName("counts");
  EXPECT_EQ(f_counts_ms.type, RowType::MAP);
  EXPECT_EQ(f_counts_ms.map_key_type, RowType::STRING);
  EXPECT_EQ(f_counts_ms.map_value_type, RowType::LONG);

  auto& f_tags_ms = ms->getFieldByName("tags");
  EXPECT_EQ(f_tags_ms.type, RowType::LIST);
  EXPECT_EQ(f_tags_ms.list_type, RowType::STRING);

  auto& f_score_ms = ms->getFieldByName("score");
  EXPECT_EQ(f_score_ms.type, RowType::DOUBLE);

  auto as = ThriftSchemaParser::parseToArrow(path, "Outer", opt);
  ASSERT_EQ(as->num_fields(), 3);
  auto f_counts = as->field(0);
  auto f_tags = as->field(1);
  auto f_score = as->field(2);
  EXPECT_EQ(f_counts->type()->id(), arrow::Type::MAP);
  auto mtype = std::static_pointer_cast<arrow::MapType>(f_counts->type());
  EXPECT_EQ(mtype->key_type()->id(), arrow::Type::STRING);
  EXPECT_EQ(mtype->item_type()->id(), arrow::Type::INT64);
  EXPECT_EQ(f_tags->type()->id(), arrow::Type::LIST);
  auto ltype = std::static_pointer_cast<arrow::ListType>(f_tags->type());
  EXPECT_EQ(ltype->value_type()->id(), arrow::Type::STRING);
  EXPECT_EQ(f_score->type()->id(), arrow::Type::FLOAT);
}
