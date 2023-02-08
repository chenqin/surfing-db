find_package(Threads REQUIRED)
include(ExternalProject)

# lib thrift name
set(THRIFT_LIBRARY libthrift)
# add_custom_target(copyConfig)

if(APPLE)
    # MacOs has problem to build thrift with libcrypto
    # install thrift through brew
    # "brew install thrift"
    # thrift
    set(THRIFT_INCLUDE_DIR /usr/local/opt/thrift@0.9/include)
    set(THRIFT_LIBRARY_PATH /usr/local/opt/thrift@0.9/lib/libthrift.a)
else()
    # build thrift
    SET(THRIFT_OPTS
            -DBUILD_TESTING=OFF
            -DBUILD_COMPILER=OFF
            -DBUILD_CPP=ON
            -DBUILD_TUTORIALS=OFF
            -DBUILD_AS3=OFF
            -DBUILD_C_GLIB=OFF
            -DBUILD_JAVA=OFF
            -DBUILD_PYTHON=OFF
            -DBUILD_HASKELL=OFF
            -DWITH_OPENSSL=OFF
            -DCMAKE_BUILD_TYPE=Release)

    ExternalProject_Add(thrift
            PREFIX thrift
            GIT_REPOSITORY https://github.com/apache/thrift.git
            CMAKE_ARGS ${THRIFT_OPTS}
            UPDATE_COMMAND ""
            INSTALL_COMMAND ""
            LOG_DOWNLOAD ON
            LOG_CONFIGURE ON
            LOG_BUILD ON
            DEPENDS ${OPENSSL_LIBRARY})

    # get source dir after download step
    ExternalProject_Get_Property(thrift SOURCE_DIR)
    ExternalProject_Get_Property(thrift BINARY_DIR)
    set(THRIFT_INCLUDE_DIR ${SOURCE_DIR}/lib/cpp/src ${BINARY_DIR})
    file(MAKE_DIRECTORY ${THRIFT_INCLUDE_DIR})
    set(THRIFT_LIBRARY_PATH ${BINARY_DIR}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}thrift.a)

endif()
message(STATUS "THRIFT_INCLUDE_DIR=${THRIFT_INCLUDE_DIR}")

add_library(${THRIFT_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${THRIFT_LIBRARY} PROPERTIES
        "IMPORTED_LOCATION" "${THRIFT_LIBRARY_PATH}"
        "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
        "INTERFACE_INCLUDE_DIRECTORIES" "${THRIFT_INCLUDE_DIR}")
