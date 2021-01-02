//
// Created by Chen Qin on 12/30/20.
//

#include <gtest/gtest.h>
#include "parser/sql.h"
#include <glog/logging.h>
#include <omp.h>

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
                auto s = sqlParser.interpret(doc);
# ifdef _OPENMP
    std::cout << "Compiled by an OpenMP-compliant implementation.";
# endif
#pragma omp parallel for
                for(int rowindex = 0 ; rowindex < 10 ; rowindex++) {
                    // apply row ser and apply transformation in parallel
                    s();
                    // apply field operation with simd
                    int c[20],a[20],b[20];
#pragma omp simd
for(int i = 0; i < 20 ; i++){
    c[i] = a[i] + b[i];
}
                    LOG(INFO) << c[0];
                }
#pragma omp barrier
            }
        }
    }
}
