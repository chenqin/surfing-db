cmake_minimum_required(VERSION 3.15)
project(surfingthriftjni-native LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(JNI REQUIRED)
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)

# Prefer system Thrift (shared, PIC) to avoid non-PIC static linking issues
find_package(PkgConfig REQUIRED)
pkg_check_modules(Thrift REQUIRED thrift)

add_library(surfingthriftjni SHARED
  src/thrift_decode_jni.cpp
  src/meta/thrift_arrow_lite.cpp
  src/parquet_jni.cpp
)
target_include_directories(surfingthriftjni PRIVATE
  ${JNI_INCLUDE_DIRS}
  ${Thrift_INCLUDE_DIRS}
  ${CMAKE_CURRENT_LIST_DIR}/include
)
target_link_libraries(surfingthriftjni
  PRIVATE Arrow::arrow_static
  PRIVATE Parquet::parquet_static
  PRIVATE ${Thrift_LIBRARIES}
)
target_compile_options(surfingthriftjni PRIVATE -Wno-unused-result)
set_target_properties(surfingthriftjni PROPERTIES OUTPUT_NAME surfingthriftjni)

# SIMD optimizations (enabled by default for performance)
option(SURF_SIMD "Enable SIMD optimizations (AVX2/SSE4.2/NEON)" ON)

if(SURF_SIMD)
  target_compile_definitions(surfingthriftjni PRIVATE SURF_SIMD=1)

  if (CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    # Enable aggressive optimizations
    target_compile_options(surfingthriftjni PRIVATE -O3 -march=native)

    # Platform-specific SIMD flags
    if (CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64|i686")
      # Try to detect and enable the best available SIMD instruction set
      include(CheckCXXCompilerFlag)

      # Check for AVX2 support
      check_cxx_compiler_flag("-mavx2" COMPILER_SUPPORTS_AVX2)
      if(COMPILER_SUPPORTS_AVX2)
        target_compile_options(surfingthriftjni PRIVATE -mavx2 -mfma)
        message(STATUS "SIMD: Enabled AVX2 optimizations")
      else()
        # Fallback to SSE4.2
        check_cxx_compiler_flag("-msse4.2" COMPILER_SUPPORTS_SSE42)
        if(COMPILER_SUPPORTS_SSE42)
          target_compile_options(surfingthriftjni PRIVATE -msse4.2)
          message(STATUS "SIMD: Enabled SSE4.2 optimizations")
        else()
          # Minimum SSSE3
          target_compile_options(surfingthriftjni PRIVATE -mssse3)
          message(STATUS "SIMD: Enabled SSSE3 optimizations")
        endif()
      endif()

      # Additional x86 optimizations
      target_compile_options(surfingthriftjni PRIVATE
        -mpopcnt           # Fast popcount
        -mbmi              # Bit manipulation instructions
        -mbmi2             # BMI2 instructions
      )

    elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "aarch64|ARM64")
      # ARM NEON is standard on AArch64
      target_compile_options(surfingthriftjni PRIVATE -march=armv8-a+simd)
      message(STATUS "SIMD: Enabled ARM NEON optimizations")

    elseif (CMAKE_SYSTEM_PROCESSOR MATCHES "arm")
      # ARM 32-bit with NEON
      check_cxx_compiler_flag("-mfpu=neon" COMPILER_SUPPORTS_NEON)
      if(COMPILER_SUPPORTS_NEON)
        target_compile_options(surfingthriftjni PRIVATE -mfpu=neon -mfloat-abi=hard)
        message(STATUS "SIMD: Enabled ARM NEON (32-bit) optimizations")
      endif()
    endif()

    # Additional performance flags
    target_compile_options(surfingthriftjni PRIVATE
      -funroll-loops         # Loop unrolling
      -ftree-vectorize       # Auto-vectorization
      -ffast-math            # Fast math (safe for our use case)
    )

  elseif(MSVC)
    # MSVC optimizations
    if (CMAKE_SYSTEM_PROCESSOR MATCHES "x86_64|AMD64")
      target_compile_options(surfingthriftjni PRIVATE /arch:AVX2 /O2 /Ot)
      message(STATUS "SIMD: Enabled AVX2 optimizations (MSVC)")
    endif()
  endif()
else()
  message(STATUS "SIMD: Disabled (use -DSURF_SIMD=ON to enable)")
endif()
