find_package(Threads REQUIRED)

include(ExternalProject)

SET(GFLAGS_OPTS
  -DGFLAGS_BUILD_SHARED_LIBS:BOOL=OFF)

ExternalProject_Add(gflags
  PREFIX gflags
  GIT_REPOSITORY https://github.com/gflags/gflags.git
  CMAKE_ARGS ${GFLAGS_OPTS}
  UPDATE_COMMAND ""
  INSTALL_COMMAND ""
  LOG_DOWNLOAD ON
  LOG_CONFIGURE ON
  LOG_BUILD ON)

# get source dir after download step
ExternalProject_Get_Property(gflags SOURCE_DIR)
ExternalProject_Get_Property(gflags BINARY_DIR)
set(GFLAGS_INCLUDE_DIRS ${BINARY_DIR}/include)
file(MAKE_DIRECTORY ${GFLAGS_INCLUDE_DIRS})
set(GFLAGS_LIBRARY_PATH ${BINARY_DIR}/lib/${CMAKE_FIND_LIBRARY_PREFIXES}gflags.a)

set(GFLAGS_LIBRARY libgflags)
add_library(${GFLAGS_LIBRARY} UNKNOWN IMPORTED)
set_target_properties(${GFLAGS_LIBRARY} PROPERTIES
    "IMPORTED_LOCATION" "${GFLAGS_LIBRARY_PATH}"
    "IMPORTED_LINK_INTERFACE_LIBRARIES" "${CMAKE_THREAD_LIBS_INIT}"
    "INTERFACE_INCLUDE_DIRECTORIES" "${GFLAGS_INCLUDE_DIRS}")
add_dependencies(${GFLAGS_LIBRARY} gflags)