cmake_minimum_required(VERSION 3.10)
project(surfingdb_core LANGUAGES CXX)

# This subproject builds the core SurfingDB libraries (meta, table, connector)
# so application binaries can simply link against `surfingdb_core`.

# Ensure the parent-defined source root is available (fallback for standalone builds)
if(NOT DEFINED MATCHA_ROOT)
  set(MATCHA_ROOT ${CMAKE_CURRENT_SOURCE_DIR}/..)
endif()
if(NOT DEFINED MATCHA_SRC)
  set(MATCHA_SRC ${MATCHA_ROOT}/src)
endif()

# Build individual components
include(${MATCHA_ROOT}/src/meta/meta.cmake)
include(${MATCHA_ROOT}/src/table/table.cmake)

set(_core_link_targets ${META} ${TABLE})
if(BUILD_CPP_KAFKA)
  include(${MATCHA_ROOT}/src/connector/connector.cmake)
  list(APPEND _core_link_targets ${CONNECTOR})
endif()

# Aggregate convenience target
add_library(surfingdb_core INTERFACE)
target_link_libraries(surfingdb_core INTERFACE ${_core_link_targets})
target_include_directories(surfingdb_core INTERFACE
  $<BUILD_INTERFACE:${MATCHA_ROOT}/include>
  $<BUILD_INTERFACE:${MATCHA_ROOT}/surfingthriftjni/native/include>
  ${DATASKETCHES_INCLUDE_DIRS}
)

# Expose component target names to parent scopes for backward compatibility
set(SURFINGDB_CORE_TARGET surfingdb_core PARENT_SCOPE)
set(META ${META} PARENT_SCOPE)
set(TABLE ${TABLE} PARENT_SCOPE)
if(BUILD_CPP_KAFKA)
  set(CONNECTOR ${CONNECTOR} PARENT_SCOPE)
endif()
