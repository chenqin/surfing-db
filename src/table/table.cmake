set(TABLE surftable)

#include_directories(${JEMALLOC_INCLUDE_DIRS})
include_directories(${DATASKETCHES_INCLUDE_DIRS})

include_directories(${THRIFT_INCLUDE_DIR})
include_directories(${SURFINGDB_SRC}/meta/gen-cpp)

if(APPLE)
    include_directories(/usr/local/include)
endif()

#find_package(Torch REQUIRED)
#include_directories(include  ~/libtorch/include)

add_library(${TABLE} STATIC
        ${SURFINGDB_SRC}/table/mrow.cpp
        ${SURFINGDB_SRC}/table/mtable.cpp
        ${SURFINGDB_SRC}/table/processors.cpp)

target_link_libraries(${TABLE}
        PRIVATE ${META}
        PRIVATE ${PARQUET_LIBRARY}
        PUBLIC ${XGBOOST_LIBRARY})

# build test binary
add_executable(TableTest
        ${SURFINGDB_SRC}/table/test/TestIntegration.cpp)

target_link_libraries(TableTest
        PRIVATE ${TABLE}
        PRIVATE ${GTEST_LIBRARY}
        PRIVATE ${GTEST_MAIN_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(TableTest TEST_LIST ALL)
