/*
 * Copyright Chen Qin on 12/30/20.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <omp.h>
#include "parser/sql.h"

namespace surfingdb {
namespace parser {
namespace test {
TEST(ParserTest, TestSQLParserJSON) {
  SQLParser sqlParser;
  rapidjson::Document doc;
  sqlParser.parser("select 1 from a", doc);
  EXPECT_EQ(doc.Size(), 1);
}

TEST(ParserTest, TestSQLInterrpret) {
  SQLParser sqlParser;
  rapidjson::Document doc;
  sqlParser.parser("create view nebula.ddl as select a, b, c from d where a=1 and b=2", doc);
  sqlParser.interpret(doc);
}
} // namespace test
} // namespace parser
} // namespace surfingdb
