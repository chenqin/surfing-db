find_package(Threads REQUIRED)

# not sure why we only add this for APPLE only.
include(ExternalProject)
ExternalProject_Add(
        DataSketches
        PREFIX datasketches
        GIT_REPOSITORY https://github.com/apache/datasketches-cpp.git
        UPDATE_COMMAND ""
        INSTALL_COMMAND ""
        BUILD_COMMAND ""
        LOG_DOWNLOAD ON
        LOG_CONFIGURE ON
        LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(DataSketches SOURCE_DIR)
ExternalProject_Get_Property(DataSketches BINARY_DIR)

set(DATASKETCHES_INCLUDE_DIRS
        ${SOURCE_DIR}/common/include
        ${SOURCE_DIR}/cpc/include
        ${SOURCE_DIR}/fi/include
        ${SOURCE_DIR}/hll/include
        ${SOURCE_DIR}/kll/include
        ${SOURCE_DIR}/sampling/include
        ${SOURCE_DIR}/theta/include
        ${SOURCE_DIR}/tuple/include)

message(STATUS "DATASKETCHES_INCLUDE_DIRS=${DATASKETCHES_INCLUDE_DIRS}")

file(MAKE_DIRECTORY ${DATASKETCHES_INCLUDE_DIRS})

#set(DATASKETCHES_LIBRARY datasketches)
#add_library(${DATASKETCHES_LIBRARY} UNKNOWN IMPORTED)

#set_target_properties(${DATASKETCHES_LIBRARY} PROPERTIES
#"INTERFACE_INCLUDE_DIRECTORIES" "${DATASKETCHES_INCLUDE_DIRS}")
#add_dependencies(${DATASKETCHES_LIBRARY} DataSketches)