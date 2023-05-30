set(TABLE surftable)

#include_directories(${JEMALLOC_INCLUDE_DIRS})
include_directories(${DATASKETCHES_INCLUDE_DIRS})

include_directories(${THRIFT_INCLUDE_DIR})
include_directories(${MATCHA_SRC}/meta/gen-cpp)

if(APPLE)
    include_directories(/usr/local/include)
endif()

#find_package(Torch REQUIRED)
#include_directories(include  ~/libtorch/include)

add_library(${TABLE} STATIC
        ${MATCHA_SRC}/table/mrow.cpp
        ${MATCHA_SRC}/table/mtable.cpp
        ${MATCHA_SRC}/table/processors.cpp)

target_link_libraries(${TABLE}
        PRIVATE ${META}
        PRIVATE ${JNI_LIBRARIES}
        PRIVATE ${PARQUET_LIBRARY}
        PRIVATE ${ARROW_LIBRARY}
        PRIVATE ${ARROW_DATASET_LIBRARY}
        PUBLIC ${XGBOOST_LIBRARY})
