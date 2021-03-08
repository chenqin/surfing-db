
#set(LIBPG_QUERY_DIR ${CMAKE_CURRENT_SOURCE_DIR}/libpg_query)
include(ExternalProject)

SET(XGB_OPTS
        -DBUILD_STATIC_LIB=OFF
        -DUSE_OPENMP=ON
        -DUSE_S3=ON)

ExternalProject_Add(xgboost
        GIT_REPOSITORY https://github.com/chenqin/xgboost
        PREFIX xgboost
        CMAKE_ARGS ${XGB_OPTS}
        INSTALL_COMMAND "" #not install to /usr/local/include/xgboost
        DEPENDS ${OPENSSL_LIBRARY}
        LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(xgboost SOURCE_DIR)
ExternalProject_Get_Property(xgboost BINARY_DIR)
message(STATUS "xgboost SOURCE=${SOURCE_DIR}")
message(STATUS "xgboost BINARY=${BINARY_DIR}")

set(XGBOOST_INCLUDE_DIR ${SOURCE_DIR}/include)
file(MAKE_DIRECTORY ${XGBOOST_INCLUDE_DIR})

message(STATUS "XGBOOST_INCLUDE_DIR=${XGBOOST_INCLUDE_DIR}")

set(XGBOOST_LIBRARY_PATH  ${SOURCE_DIR}/lib/libxgboost.so)
message(STATUS "XGBOOST_LIBRARY_PATH=${XGBOOST_LIBRARY_PATH}")

set(XGBOOST_LIBRARY XGBOOST_LIBRARY_PATH)
add_library(${XGBOOST_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${XGBOOST_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${XGBOOST_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${XGBOOST_INCLUDE_DIR}")
