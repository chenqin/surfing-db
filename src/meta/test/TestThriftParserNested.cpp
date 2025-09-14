#include <gtest/gtest.h>
#include <fstream>
#include <arrow/api.h>

#include "meta/thrift_parser.h"

using namespace matcha::meta;

static std::string writeTempThrift2(const std::string& content) {
  char path[] = "/tmp/test_schema_nested_XXXXXX";
  int fd = mkstemp(path);
  close(fd);
  std::ofstream out(path);
  out << content;
  out.close();
  return std::string(path);
}

TEST(ThriftParserNestedTest, StructReferencesAndCollections) {
  std::string thrift = R"IDL(
    struct Inner {
      1: i64 id,
      2: string note
    }

    struct Outer {
      1: Inner inner,
      2: list<Inner> inners,
      3: map<string, Inner> lookup
    }
  )IDL";
  auto path = writeTempThrift2(thrift);
  ThriftParseOptions opt;
  auto as = ThriftSchemaParser::parseToArrow(path, "Outer", opt);
  ASSERT_EQ(as->num_fields(), 3);

  auto f0 = as->field(0);
  EXPECT_EQ(f0->name(), "inner");
  ASSERT_EQ(f0->type()->id(), arrow::Type::STRUCT);
  auto st = std::static_pointer_cast<arrow::StructType>(f0->type());
  ASSERT_EQ(st->num_fields(), 2);
  EXPECT_EQ(st->field(0)->name(), "id");
  EXPECT_EQ(st->field(0)->type()->id(), arrow::Type::INT64);
  EXPECT_EQ(st->field(1)->name(), "note");
  EXPECT_EQ(st->field(1)->type()->id(), arrow::Type::STRING);

  auto f1 = as->field(1);
  ASSERT_EQ(f1->type()->id(), arrow::Type::LIST);
  auto lt = std::static_pointer_cast<arrow::ListType>(f1->type());
  ASSERT_EQ(lt->value_type()->id(), arrow::Type::STRUCT);

  auto f2 = as->field(2);
  ASSERT_EQ(f2->type()->id(), arrow::Type::MAP);
  auto mt = std::static_pointer_cast<arrow::MapType>(f2->type());
  EXPECT_EQ(mt->key_type()->id(), arrow::Type::STRING);
  ASSERT_EQ(mt->item_type()->id(), arrow::Type::STRUCT);
}

