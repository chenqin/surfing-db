cmake_minimum_required(VERSION 3.15)
project(surfingthriftjni-native LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(JNI REQUIRED)
find_package(Arrow REQUIRED)

# Prefer system Thrift (shared, PIC) to avoid non-PIC static linking issues
find_package(PkgConfig REQUIRED)
pkg_check_modules(Thrift REQUIRED thrift)

add_library(surfingthriftjni SHARED
  src/thrift_decode_jni.cpp
  src/meta/thrift_arrow_lite.cpp
)
target_include_directories(surfingthriftjni PRIVATE
  ${JNI_INCLUDE_DIRS}
  ${Thrift_INCLUDE_DIRS}
  ${CMAKE_CURRENT_LIST_DIR}/include
)
target_link_libraries(surfingthriftjni
  PRIVATE Arrow::arrow_static
  PRIVATE ${Thrift_LIBRARIES}
)
target_compile_options(surfingthriftjni PRIVATE -Wno-unused-result)
set_target_properties(surfingthriftjni PROPERTIES OUTPUT_NAME surfingthriftjni)

# Optional SIMD optimizations
option(SURF_SIMD "Enable SIMD optimizations" OFF)
if(SURF_SIMD)
  target_compile_definitions(surfingthriftjni PRIVATE SURF_SIMD=1)
  # Favor aggressive optimization when SIMD enabled
  if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    target_compile_options(surfingthriftjni PRIVATE -O3)
    if (CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
      target_compile_options(surfingthriftjni PRIVATE -mavx2)
    endif()
  endif()
endif()
