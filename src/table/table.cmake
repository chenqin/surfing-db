# define ingestion module in nebula
# this module can be run as standalone app or runtime in nebula service
# it is responsible for single ingest spec or task split
set(TABLE surftable)

find_package(MPI REQUIRED)

set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREADED ON)
set(Boost_USE_STATIC_RUNTIME OFF)
find_package(Boost REQUIRED mpi serialization)

include_directories(${JEMALLOC_INCLUDE_DIRS})
include_directories(${DATASKETCHES_INCLUDE_DIRS})

# build nebula.ingest library
add_library(${TABLE} STATIC ${SURFINGDB_SRC}/table/Node.cpp)

target_link_libraries(${TABLE}
        PUBLIC ${JEMALLOC_LIBRARIES}
        PUBLIC Threads::Threads
        PUBLIC OpenMP::OpenMP_CXX
        PUBLIC ${MPI_CXX_INCLUDE_PATH}
        PUBLIC ${GLOG_LIBRARY}
        PUBLIC ${GFLAGS_LIBRARY}
        PUBLIC ${JSON_LIBRARY}
        PUBLIC ${ARROW_LIBRARY}
        PUBLIC ${PARQUET_LIBRARY})

# build test binary
add_executable(TableTest
        ${SURFINGDB_SRC}/table/test/TestIntegration.cpp)

target_link_libraries(TableTest
        PRIVATE Threads::Threads
        PRIVATE OpenMP::OpenMP_CXX
        PRIVATE MPI::MPI_CXX
        PRIVATE ${TABLE}
        PRIVATE ${JSON_LIBRARY}
        PRIVATE ${GTEST_LIBRARY}
        PRIVATE ${GTEST_MAIN_LIBRARY}
        PRIVATE ${ARROW_LIBRARY}
        PRIVATE ${PARQUET_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(TableTest TEST_LIST ALL)
