find_package(Threads REQUIRED)
include(ExternalProject)
set(DUCKDB_LIBRARY libduckdb)
SET(DUCKDB_OPTS
        -DDUCKDB_BUILD_LIBRARY:BOOL=ON)

ExternalProject_Add(duckdb
        PREFIX duckdb
        GIT_REPOSITORY https://github.com/chenqin/duckdb
        UPDATE_COMMAND ""
        INSTALL_COMMAND ""
        CMAKE_ARGS ${DUCKDB_OPTS}
        LOG_DOWNLOAD ON
        LOG_CONFIGURE ON
        LOG_BUILD ON
        DEPENDS ${OPENSSL_LIBRARY})

# get source dir after download step
ExternalProject_Get_Property(duckdb SOURCE_DIR)
ExternalProject_Get_Property(duckdb BINARY_DIR)
set(DUCKDB_INCLUDE_DIR ${SOURCE_DIR}/src/include)
file(MAKE_DIRECTORY ${DUCKDB_INCLUDE_DIR})
set(DUCKDB_LIBRARY_PATH ${BINARY_DIR}/src/${CMAKE_FIND_LIBRARY_PREFIXES}duckdb.so)

message(STATUS "DUCKDB_INCLUDE_DIR=${DUCKDB_INCLUDE_DIR}")
message(STATUS "DUCKDB_LIBRARY_PATH=${DUCKDB_LIBRARY_PATH}")

add_library(${DUCKDB_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${DUCKDB_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${DUCKDB_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${DUCKDB_INCLUDE_DIR}")
