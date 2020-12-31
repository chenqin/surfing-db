# define ingestion module in nebula
# this module can be run as standalone app or runtime in nebula service
# it is responsible for single ingest spec or task split
set(TABLE surftable)

# build nebula.ingest library
add_library(${TABLE} STATIC
        ${SURFINGDB_SRC}/table/table.cpp)

target_link_libraries(${TABLE}
        PUBLIC ${GLOG_LIBRARY}
        PUBLIC ${GFLAGS_LIBRARY}
        PUBLIC ${JSON_LIBRARY}
        PUBLIC ${FLATBUFFERS_LIBRARY})

# build test binary
add_executable(TableTest
        ${SURFINGDB_SRC}/table/test/TestIntegration.cpp)

target_link_libraries(TableTest
        PRIVATE OpenMP::OpenMP_CXX
        PRIVATE ${TABLE}
        PRIVATE ${JSON_LIBRARY}
        PRIVATE ${GTEST_LIBRARY}
        PRIVATE ${GTEST_MAIN_LIBRARY}
        PRIVATE ${FLATBUFFERS_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(TableTest TEST_LIST ALL)
