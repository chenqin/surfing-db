# define ingestion module in nebula
# this module can be run as standalone app or runtime in nebula service
# it is responsible for single ingest spec or task split
set(CONNECTOR surfconnector)

#execute_process(COMMAND sudo apt remove librdkafka-pin)
#execute_process(COMMAND sudo apt install librdkafka++-dev)
# build nebula.ingest library
add_library(${CONNECTOR} STATIC
                ${MATCHA_SRC}/connector/kafka.cpp 
                ${MATCHA_SRC}/connector/datagen.cpp)

set_target_properties(${CONNECTOR} PROPERTIES POSITION_INDEPENDENT_CODE ON)

include_directories(${KC_INCLUDE_DIRS})

target_link_libraries(${CONNECTOR}
        PRIVATE ${META}
        PRIVATE ${TABLE}
        PRIVATE ${PARQUET_LIBRARY}
        PRIVATE ${KC_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
