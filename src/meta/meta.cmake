# define ingestion module in nebula
# this module can be run as standalone app or runtime in nebula service
# it is responsible for single ingest spec or task split
set(META surfmeta)

# generate schema skeleton
#execute_process(COMMAND thrift --gen cpp ${MATCHA_SRC}/meta/schema.thrift)

# build nebula.ingest library
add_library(${META} STATIC
        ${MATCHA_SRC}/meta/gen-cpp
        ${MATCHA_SRC}/meta/node.cpp
        ${MATCHA_SRC}/meta/schema.cpp
        ${MATCHA_SRC}/meta/thrift_parser.cpp)

set_target_properties(${META} PROPERTIES POSITION_INDEPENDENT_CODE ON)

# define basic package dependencies
target_link_libraries(${META}
        PUBLIC Threads::Threads
        PUBLIC MPI::MPI_CXX
        PUBLIC OpenMP::OpenMP_CXX
        PUBLIC ${GLOG_LIBRARY}
        PUBLIC ${GFLAGS_LIBRARY}
        PUBLIC ${ARROW_LIBRARY}
        PRIVATE ${JNI_LIBRARIES}
        PUBLIC ${THRIFT_LIBRARY})

# discover all gtests in this module
include(GoogleTest)

# CLI tool: thrift2arrow
add_executable(thrift2arrow ${MATCHA_SRC}/meta/tools/thrift2arrow.cpp)
target_link_libraries(thrift2arrow
        PRIVATE ${META}
        PRIVATE ${ARROW_LIBRARY})
