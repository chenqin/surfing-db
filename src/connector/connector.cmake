# define ingestion module in nebula
# this module can be run as standalone app or runtime in nebula service
# it is responsible for single ingest spec or task split
set(CONNECTOR surfconnector)

#execute_process(COMMAND sudo apt remove librdkafka-pin)
#execute_process(COMMAND sudo apt install librdkafka++-dev)
# build nebula.ingest library
add_library(${CONNECTOR} STATIC
        ${SURFINGDB_SRC}/connector/kafka.cpp ${SURFINGDB_SRC}/connector/datagen.cpp)

include_directories(${KC_INCLUDE_DIRS})
include_directories(${ZOOKEEPER_INCLUDE_DIR})

target_link_libraries(${CONNECTOR}
        PUBLIC ${META}
        PRIVATE ${TABLE}
        PUBLIC glog::glog
        PUBLIC ${GFLAGS_LIBRARY}
        PUBLIC ${JSON_LIBRARY}
        PUBLIC ${THRIFT_LIBRARY}
        PUBLIC ${ARROW_LIBRARY}
        PUBLIC ${PARQUET_LIBRARY}
        PUBLIC rdkafka)

# discover all gtests in this module
include(GoogleTest)
