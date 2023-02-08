# define ingestion module in nebula
# this module can be run as standalone app or runtime in nebula service
# it is responsible for single ingest spec or task split
set(META surfmeta)

# generate schema skeleton
#execute_process(COMMAND thrift --gen cpp ${SURFINGDB_SRC}/meta/schema.thrift)

# build nebula.ingest library
add_library(${META} STATIC
        ${SURFINGDB_SRC}/meta/gen-cpp
        ${SURFINGDB_SRC}/meta/node.cpp
        ${SURFINGDB_SRC}/meta/schema.cpp)

target_link_libraries(${META}
        PUBLIC glog::glog
        PUBLIC ${THRIFT_LIBRARY}
        PUBLIC ${GFLAGS_LIBRARY}
        PUBLIC ${JSON_LIBRARY}
        PUBLIC ${FMT_LIBRARY}
        PUBLIC ${DUCKDB_LIBRARY}
        PUBLIC ${ARROW_LIBRARY_TARGET})

# discover all gtests in this module
include(GoogleTest)
