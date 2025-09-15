#include <gtest/gtest.h>
#include <arrow/api.h>

#include "meta/thrift_parser.h"

using namespace matcha::meta;

TEST(ThriftParserMabs, ParseMabsMetricsArrow) {
  ThriftParseOptions opt;
  auto schema = ThriftSchemaParser::parseToArrow("src/mabs.thrift", "MabsMetrics", opt);
  ASSERT_TRUE(schema);
  // Basic checks
  EXPECT_TRUE(schema->GetFieldByName("timestamp"));
  EXPECT_TRUE(schema->GetFieldByName("service_tags"));
  EXPECT_TRUE(schema->GetFieldByName("node_tags"));
  EXPECT_TRUE(schema->GetFieldByName("counters"));
  EXPECT_TRUE(schema->GetFieldByName("gauges"));
  EXPECT_TRUE(schema->GetFieldByName("histograms"));
  EXPECT_TRUE(schema->GetFieldByName("service_name"));
  EXPECT_TRUE(schema->GetFieldByName("double_counters"));
}
