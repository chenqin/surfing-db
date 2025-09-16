cmake_minimum_required(VERSION 3.15)
project(surfingthriftjni-native LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(JNI REQUIRED)
find_package(Arrow REQUIRED)

# Reuse the repo's Thrift external build helper to provide THRIFT_LIBRARY/THRIFT_INCLUDE_DIR
get_filename_component(REPO_ROOT "${CMAKE_CURRENT_LIST_DIR}/.." ABSOLUTE)
include("${REPO_ROOT}/../ext/Thrift_Ext.cmake")

add_library(surfingthriftjni SHARED src/thrift_decode_jni.cpp)
target_include_directories(surfingthriftjni PRIVATE ${JNI_INCLUDE_DIRS})
target_link_libraries(surfingthriftjni
  PRIVATE Arrow::arrow
  PRIVATE ${THRIFT_LIBRARY}
  )
set_target_properties(surfingthriftjni PROPERTIES OUTPUT_NAME surfingthriftjni)
