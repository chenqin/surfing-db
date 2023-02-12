# define ingestion module in nebula
# this module can be run as standalone app or runtime in nebula service
# it is responsible for single ingest spec or task split
set(TABLE surftable)

#include_directories(${JEMALLOC_INCLUDE_DIRS})
include_directories(${DATASKETCHES_INCLUDE_DIRS})

include_directories(${THRIFT_INCLUDE_DIR})
include_directories(${SURFINGDB_SRC}/table/gen-cpp)

if(APPLE)
    include_directories(/usr/local/include)
endif()

add_library(${TABLE} STATIC
        ${SURFINGDB_SRC}/table/mrow.cpp
        ${SURFINGDB_SRC}/table/mtable.cpp
        ${SURFINGDB_SRC}/table/processors.cpp)

target_link_libraries(${TABLE}
        PUBLIC Threads::Threads
        PUBLIC MPI::MPI_CXX
        PUBLIC OpenMP::OpenMP_CXX
        PUBLIC ${OPENSSL_LIBRARY}
        PUBLIC ${CRYPTO_LIBRARY}
        PUBLIC ${THRIFT_LIBRARY}
        PUBLIC glog::glog
        PUBLIC ${GFLAGS_LIBRARY}
        PUBLIC ${JSON_LIBRARY}
        PUBLIC ${ARROW_LIBRARY}
        PUBLIC ${PARQUET_LIBRARY}
        PUBLIC ${XGBOOST_LIBRARY})

# build test binary
add_executable(TableTest
        ${SURFINGDB_SRC}/table/test/TestIntegration.cpp)

target_link_libraries(TableTest
        PRIVATE ${META}
        PRIVATE ${TABLE}
        PRIVATE ${JSON_LIBRARY}
        PRIVATE ${GTEST_LIBRARY}
        PRIVATE ${GTEST_MAIN_LIBRARY}
        PRIVATE ${ARROW_LIBRARY}
        PRIVATE ${PARQUET_LIBRARY}
        PRIVATE ${XGBOOST_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(TableTest TEST_LIST ALL)
