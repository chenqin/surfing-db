find_package(Threads REQUIRED)
include(ExternalProject)
set(ZK_LIBRARY libzookeeper)

ExternalProject_Add(zookeeper
        PREFIX zookeeper
        GIT_REPOSITORY git@github.com:chenqin/libzk.git
        GIT_TAG main
        UPDATE_COMMAND ""
        INSTALL_COMMAND ""
        LOG_DOWNLOAD ON
        LOG_CONFIGURE ON
        LOG_BUILD ON
        DEPENDS ${OPENSSL_LIBRARY})

# get source dir after download step
ExternalProject_Get_Property(zookeeper SOURCE_DIR)
ExternalProject_Get_Property(zookeeper BINARY_DIR)
set(ZOOKEEPER_INCLUDE_DIR ${SOURCE_DIR}/include)
file(MAKE_DIRECTORY ${ZOOKEEPER_INCLUDE_DIR})
set(ZOOKEEPER_LIBRARY_PATH ${BINARY_DIR}/${CMAKE_FIND_LIBRARY_PREFIXES}zookeeper.a)

message(STATUS "ZOOKEEPER_INCLUDE_DIR=${ZOOKEEPER_INCLUDE_DIR}")
message(STATUS "ZOOKEEPER_LIBRARY_PATH=${ZOOKEEPER_LIBRARY_PATH}")
set(ZOOKEEPER_LIBRARY ZOOKEEPER_LIBRARY_PATH)
add_library(${ZOOKEEPER_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${ZOOKEEPER_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${ZOOKEEPER_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${DUCKDB_INCLUDE_DIR}")
