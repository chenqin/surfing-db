//
// Created by Chen Qin on 12/30/20.
//

#include <gtest/gtest.h>
#include "parser/sql.h"
#include <glog/logging.h>

namespace surfingdb{
    namespace parser {
        namespace test {
            TEST(ParserTest, TestSQLParserJSON) {
                SQLParser sqlParser;
                rapidjson::Document  doc;
                sqlParser.parser("select 1 from a", doc);
                EXPECT_EQ(doc.Size(), 1);
            }

            TEST(ParserTest, TestSQLInterrpret) {
                SQLParser sqlParser;
                rapidjson::Document  doc;
                sqlParser.parser("create view nebula.ddl as select a, b, c from d where a=1 and b=2", doc);
                sqlParser.interpret(doc);
            }
        }
    }
}
