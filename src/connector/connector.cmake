# define ingestion module in nebula
# this module can be run as standalone app or runtime in nebula service
# it is responsible for single ingest spec or task split
set(CONNECTOR surfconnector)

execute_process(COMMAND sudo apt remove librdkafka-pin)

# build nebula.ingest library
add_library(${CONNECTOR} STATIC
        ${SURFINGDB_SRC}/connector/kafka.cpp)

target_link_libraries(${CONNECTOR}
        PUBLIC ${META}
        PUBLIC ${GLOG_LIBRARY}
        PUBLIC ${GFLAGS_LIBRARY}
        PUBLIC ${JSON_LIBRARY}
        PUBLIC ${THRIFT_LIBRARY}
        PUBLIC ${ARROW_LIBRARY}
        PUBLIC ${KAFKA_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
