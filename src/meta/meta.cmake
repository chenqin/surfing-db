# define ingestion module in nebula
# this module can be run as standalone app or runtime in nebula service
# it is responsible for single ingest spec or task split
set(META surfmeta)

# build nebula.ingest library
add_library(${META} STATIC
        ${SURFINGDB_SRC}/meta/node.cpp)

target_link_libraries(${META}
        PUBLIC ${GLOG_LIBRARY}
        PUBLIC ${GFLAGS_LIBRARY}
        PUBLIC ${JSON_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
