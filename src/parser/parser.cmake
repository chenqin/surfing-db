# define ingestion module in nebula
# this module can be run as standalone app or runtime in nebula service
# it is responsible for single ingest spec or task split
set(PARSER surfparser)

add_library(${PARSER} STATIC
        ${SURFINGDB_SRC}/parser/sql.cpp)

target_link_libraries(${PARSER}
        PUBLIC ${GLOG_LIBRARY}
        PUBLIC ${GFLAGS_LIBRARY}
        PUBLIC ${JSON_LIBRARY}
        PUBLIC ${LIBPG_QUERY_LIBRARY})

# build test binary
add_executable(ParserTest
        ${SURFINGDB_SRC}/parser/test/TestIntegration.cpp)

target_link_libraries(ParserTest
        PRIVATE OpenMP::OpenMP_CXX
        PRIVATE ${PARSER}
        PRIVATE glog::glog
        PRIVATE ${JSON_LIBRARY}
        PRIVATE ${GTEST_LIBRARY}
        PRIVATE ${GTEST_MAIN_LIBRARY}
        PRIVATE ${LIBPG_QUERY_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(ParserTest TEST_LIST ALL)
