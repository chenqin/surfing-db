# Require CMake 3.16+ on Mac OSX, as previous versions of CMake had trouble locating
# OpenMP on some environments
cmake_minimum_required(VERSION 3.10)
project(matcha LANGUAGES CXX VERSION 0.1)


option(ENABLE_MPI_TESTS "Enable MPI-based shuffle tests" OFF)

find_package(Java 1.8 REQUIRED)

if (linux)
    project(Treads)
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
    set(THREADS_PREFER_PTHREAD_FLAG TRUE)
    set(CMAKE_CXX_STANDARD 17)
    set(CMAKE_CXX_STANDARD_REQUIRED ON)
    set(CMAKE_VERBOSE_MAKEFILE ON)
endif ()

find_package(Threads REQUIRED)
find_package(MPI REQUIRED)
find_package(Boost REQUIRED)
find_package(CURL REQUIRED)
find_package(JNI REQUIRED)
include_directories(${JNI_INCLUDE_DIRS})


#find_package(Torch REQUIRED)
find_package(ArrowDataset REQUIRED)
set(ARROW_DATASET_LIBRARY ArrowDataset::arrow_dataset_static)
find_package(Thrift QUIET)
if(Thrift_FOUND)
  set(THRIFT_LINK_LIBS Thrift::thrift)
else()
  find_package(PkgConfig REQUIRED)
  pkg_check_modules(THRIFT REQUIRED thrift)
  set(THRIFT_LINK_LIBS ${THRIFT_LIBRARIES})
  include_directories(${THRIFT_INCLUDE_DIRS})
endif()
# Some targets depend on a CMake target named "thrift". Create a stub if the
# package did not provide one so add_dependencies() still works.
if(NOT TARGET thrift)
  add_custom_target(thrift)
  if(TARGET Thrift::thrift)
    add_dependencies(thrift Thrift::thrift)
  endif()
endif()
find_package(GTest REQUIRED)

if(APPLE)
    set(OPENSSL_ROOT_DIR /opt/homebrew/Cellar/openssl@3/3.0.8)
    set(OPENSSL_LIBRARIES ${OPENSSL_ROOT_DIR}/lib)
    set(OPENSSL_INCLUDE_DIR ${OPENSSL_ROOT_DIR}/include)
    set(OPENSSL_CRYPTO_LIBRARY ${OPENSSL_ROOT_DIR}/lib)

    set(BOOST_ROOT_DIR /opt/homebrew/Cellar/boost/1.81.0_1)
    set(BOOST_INCLUDE_DIR /opt/homebrew/Cellar/boost/1.81.0_1/include/)
    include_directories(/opt/homebrew/Cellar/boost/1.81.0_1/include/)
    set(BOOST_LIBRARIES /opt/homebrew/Cellar/boost/1.81.0_1/lib/)

    set(glog_INCLUDE_DIR /opt/homebrew/Cellar/glog/0.6.0/include/)
    set(GLOG_LIBRARY /opt/homebrew/Cellar/glog/0.6.0/lib/libglog.dylib)
    set(glog_INCLUDE_DIRS ${glog_INCLUDE_DIR})

    #/opt/homebrew/Cellar/googletest/1.13.0
else()
    #find_package (Glog REQUIRED)
    find_package(PkgConfig REQUIRED)
    pkg_check_modules(glog REQUIRED libglog)
    set(GLOG_LIBRARY ${glog_LIBRARIES})
endif()

#find_package(OpenMP REQUIRED)
find_package(OpenSSL REQUIRED)
#https://pybind11.readthedocs.io/en/stable/advanced/embedding.html
find_package(pybind11 REQUIRED)

set(MATCHA_ROOT ${PROJECT_SOURCE_DIR})
set(MATCHA_SRC ${MATCHA_ROOT}/src)

#SET(CMAKE_C_FLAGS ${CMAKE_C_FLAGS} -fopenmp)

include_directories(include ${glog_INCLUDE_DIRS} ${MATCHA_SRC} ${MPI_CXX_INCLUDE_PATH} ${MPI_INCLUDE_PATH} ~/libtorch/include)

# a switch to use clang or g++
# clang or g++
# this switch only works on linux
set(NC gcc)
if (APPLE)
    set(NCXX /usr/bin/g++)
    set(NCC /usr/bin/gcc)
    set(NCFLAGS "-Wall -Wextra -Werror -Wno-deprecated -Wno-error=extern-c-compat -Wno-error=inconsistent-missing-override -Wno-error=nullability-completeness -Wno-error=sign-compare -Wno-error=unknown-warning-option")
    set(NCFLAGS_RELEASE "-O3")
else ()
    if (${NC} STREQUAL "clang")
        set(NCXX /usr/bin/clang++)
        set(NCC /usr/bin/clang)
        set(NCFLAGS "-Wall -Wextra -Werror -I/usr/include/ -I/usr/local/include")
        set(NCFLAGS_RELEASE "-O3")
    else ()
        # this build branch is the final release version
        set(NCXX /usr/bin/g++)
        set(NCC /usr/bin/gcc)
        list(APPEND LNCFLAGS
                "-Wall -Wextra -lstdc++ -lstdc++fs"
                "-Wl,--no-as-needed -no-pie -ldl"
                "-I/usr/include/ -I/usr/local/include -L/usr/local/lib -L/usr/lib -I/usr/include/python3.10")

        # in my testing with aggregation func, tcmalloc performs better than jemalloc
        # turn it on as default for now
        if (NOT PPROF)
            set(PPROF 2)
        endif ()

        # add a build option to enable native optimization
        if (NAT STREQUAL "1")
            set(NAT "-march=native")
        else ()
            set(NAT "")
        endif ()

        # TODO(cao) - fix this build option
        # enable LTO on build option
        if (LTO STREQUAL "1")
            set(LTO "-flto")
        else ()
            set(LTO "")
        endif ()

        # by default - do not stripe symbols in release, only when specified to reduce binary size
        if (SYM STREQUAL "1")
            set(SYM "-s")
        else ()
            set(SYM "")
        endif ()

        # turn on GDB debug info if using gdb
        if (GDB STREQUAL "1")
            set(GDB "-g")
        else ()
            set(GDB "")
        endif ()

        # build flag -DGPROF
        if (GPROF STREQUAL "1")
            # using gprof
            list(APPEND LNCFLAGS
                    "-pg -DGPROF")
            # build flag -DPPROF -> gperftools
        elseif (PPROF STREQUAL "1")
            # or using gperftools, ask each target to depends on perftools_library for linking
            # unwind is needed by gperftools
            list(APPEND LNCFLAGS
                    "-lunwind"
                    "-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free"
                    "-DPPROF")
        elseif (PPROF STREQUAL "2")#include <arrow.hpp>
            # or using gperftools, ask each target to depends on perftools_library for linking
            # unwind is needed by gperftools
            list(APPEND LNCFLAGS
                    "-lunwind"
                    "-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
        else ()
            # strip symbols since we don't need profiling and use jemalloc by default on linux release
            # MALLOC_CONF=stats_print:true ./NodeServer to enable jemalloc stats
            list(APPEND LNCFLAGS
                    "-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
        endif ()

        string(REPLACE ";" " " NCFLAGS "${LNCFLAGS}")
        # -Ofast vs -O3
        # HashFlat.genCopier will crash by calling ValueEval.merge for int128 typed avg function
        # I have spent much time to figure out why but no luck yet, need some more help in the future.
        # optimizations working fine: -O0, -O1, -Os
        # -O2, -O3, -Ofast will crash
        # Perf Benchmark(6 tests): ./ApiTests --gtest_filter=ApiTest.*:-ApiTest.TestAvgAggregation
        # interesting, by running an end2end query test, -O1 actually runs faster than -O2 on the same box.
        # also try optimizations for size (Os)
        # -O1: 800ms, 797ms, 807ms, 808ms, 801ms
        # -O2: 892ms, 871ms, 898ms, 881ms, 871ms
        # -O3: 608ms, 594ms, 605ms, 600ms, 599ms
        # -Ofast: 596ms, 608ms, 596ms, 594ms, 603ms
        # -Os: 861ms, 861ms, 865ms, 866ms, 843ms
        # figure out flags differences between optimization level:
        # dump flags for o1: gcc -O1 -Q --help=optimizers -xc - < /dev/null > o1.txt
        # dump flags for o2: gcc -O2 -Q --help=optimizers -xc - < /dev/null > o2.txt
        # diff them
        # for libfmt, build with header only, we can append this at the tail "-DFMT_HEADER_ONLY=1"
        # About compability, if we specify -march=native, gcc will optimize code for current build machine
        # we can use this command to figure out what the real vaue of arch is `gcc -march=native -Q --help=target|grep march`
        # On my daily dev machine, it gives me this value: skylake-avx512
        # In another machine, I found it as "haswell", and this may lead crash with `illegal instruction` failure.
        # So to make sure the published image works more broadly, we may want to remove this `-march=native`, or specify the target value
        set(NCFLAGS_RELEASE "-Ofast ${NAT} ${SYM} ${LTO} ${GDB}")
    endif ()
endif ()

# by default using clang on mac and gcc on linux
set(CMAKE_CXX_COMPILER ${NCXX})
set(CMAKE_CC_COMPILER ${NCXX})
set(CMAKE_C_COMPILER ${NCC})

if (NOT CMAKE_BUILD_TYPE)
    if (APPLE)
        set(CMAKE_BUILD_TYPE Debug)
    else ()
        set(CMAKE_BUILD_TYPE Release)
    endif ()
endif ()

set(CMAKE_CXX_FLAGS ${NCFLAGS})
set(CMAKE_CXX_FLAGS_DEBUG "-g -DNDBG")
set(CMAKE_CXX_FLAGS_RELEASE ${NCFLAGS_RELEASE})

find_package(MPI REQUIRED)
set(ENV{MPI_HOSTS} ${PROJECT_SOURCE_DIR}/host_file)

# we use static link always
set(BUILD_SHARED_LIBS OFF)

# set cellar path for libries installed by brew
if (APPLE)
    set(DL_EXT "dylib")
    set(CELLAR_ROOT /usr/local/Cellar)
else ()
    set(DL_EXT "so")
    set(CELLAR_ROOT /home/linuxbrew/.linuxbrew/Cellar)
endif ()

#include(ext/FMT_Ext.cmake)

# add a basic gtest dependency
enable_testing()

# include utils
include(ext/OPENSSL_Ext.cmake)

#set(PYTHON_INCLUDE_DIRS ${PYTHON_INCLUDE_DIRS} /home/chen/.local/lib/python3.10/site-packages/numpy/core/include)
#include_directories(include ${PYTHON_INCLUDE_DIRS})

#arrow
find_package(Arrow REQUIRED)
set(ARROW_LIBRARY Arrow::arrow_static)
 
find_package(Parquet REQUIRED)
set(PARQUET_LIBRARY Parquet::parquet_static)


find_package(Python3 COMPONENTS Interpreter Development)

# include ML libs
#find_package(lz4)
## Removed C++ Kafka connector; Java-native consumer is used instead

# set up directory to search for headers

#include(ext/PyTorch_Ext.cmake)

option(BUILD_CPP_KAFKA "Build C++ Kafka-based executables" OFF)

add_subdirectory(core)

# build test binary
if(BUILD_CPP_KAFKA)
  add_executable(FlinkJobWatcher src/FlinkJobWatcher.cpp)
  add_dependencies(FlinkJobWatcher thrift)
endif()

add_executable(LocalCluster ${MATCHA_SRC}/LocalCluster.cpp)

if(BUILD_CPP_KAFKA)
  add_executable(MABS 
      ${MATCHA_SRC}/mabs/mabs_types.cpp
      ${MATCHA_SRC}/mabs/mabs_constants.cpp
      ${MATCHA_SRC}/MABS.cpp)
  add_dependencies(MABS thrift)
endif()

# workaround make issue for now
if(BUILD_CPP_KAFKA)
  add_custom_command(TARGET FlinkJobWatcher PRE_BUILD
          COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target thrift)

  add_custom_command(TARGET FlinkJobWatcher PRE_BUILD
          COMMAND cd ${CMAKE_SOURCE_DIR}/drsquirrel-java-project && mvn clean package -Darrow.version=${ARROW_VERSION} 
          COMMAND cp ${CMAKE_SOURCE_DIR}/drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar ${CMAKE_SOURCE_DIR}/surfing-db-java.jar)
endif()

if(BUILD_CPP_KAFKA)
  set_target_properties(MABS PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")
  target_link_libraries(MABS
          PRIVATE surfingdb_core
          PRIVATE ${ARROW_LIBRARY}
          PRIVATE ${THRIFT_LINK_LIBS})
endif()

if(BUILD_CPP_KAFKA)
  set_target_properties(FlinkJobWatcher PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")
  target_link_libraries(FlinkJobWatcher
      PRIVATE surfingdb_core
      PRIVATE ${ARROW_LIBRARY})
endif()

set_target_properties(LocalCluster PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")

target_link_libraries(LocalCluster
    PRIVATE surfingdb_core
    PRIVATE ${ARROW_LIBRARY})

## Kafka JNI bridge removed; Java-native Kafka consumer is used instead

# JNI library for processors
add_library(surfingprocessorsjni SHARED ${MATCHA_SRC}/table/processors_jni.cpp)
target_link_libraries(surfingprocessorsjni
    PRIVATE surfingdb_core
    PRIVATE ${ARROW_LIBRARY}
    PRIVATE ${PARQUET_LIBRARY}
    PRIVATE ${GLOG_LIBRARY})
set_target_properties(surfingprocessorsjni PROPERTIES OUTPUT_NAME surfingprocessorsjni)

# Python module exposing the same processor primitives via pybind11
pybind11_add_module(surfingprocessorspy MODULE ${MATCHA_SRC}/table/processors_py.cpp)
target_link_libraries(surfingprocessorspy
    PRIVATE surfingdb_core
    PRIVATE ${ARROW_LIBRARY}
    PRIVATE ${PARQUET_LIBRARY}
    PRIVATE ${GLOG_LIBRARY}
    PRIVATE MPI::MPI_CXX)

# JNI library for Thrift -> Arrow decoding has moved to the Maven submodule
# surfingthriftjni/, which builds the native library via its own CMake under
# surfingthriftjni/native and packages natives into a classifier JAR.
#
# If you need to build the JNI decoder, use:
#   mvn -q -f surfingthriftjni/pom.xml -DskipTests package

# -----------------------------
# C++ Tests
# -----------------------------
include(GoogleTest)

# Full test binary (integration + others)
add_executable(SurfingDbTests
    ${MATCHA_SRC}/table/test/TestIntegration.cpp
    ${MATCHA_SRC}/table/test/TestArrowSpill.cpp)
target_link_libraries(SurfingDbTests
    PRIVATE surfingdb_core
    PRIVATE ${ARROW_LIBRARY}
    PRIVATE ${PARQUET_LIBRARY}
    PRIVATE GTest::gtest_main
    PRIVATE GTest::gtest)
gtest_discover_tests(SurfingDbTests
  DISCOVERY_MODE PRE_TEST
)

# Convenience wrapper to run only the Arrow spill test via CTest
add_test(NAME ArrowSpillTest
  COMMAND SurfingDbTests --gtest_filter=ArrowSpillTest.*)
set_tests_properties(ArrowSpillTest PROPERTIES LABELS "unit;arrow;spill")

# (CUDA support removed)

# Focused thrift parser tests (no table/connector deps)
add_executable(ThriftParserTests
    ${MATCHA_SRC}/meta/test/TestThriftParser.cpp
    ${MATCHA_SRC}/meta/test/TestThriftParserNested.cpp
    ${MATCHA_SRC}/meta/test/TestThriftParserFlatten.cpp
    ${MATCHA_SRC}/meta/test/TestThriftParserFlattenAdvanced.cpp
    ${MATCHA_SRC}/meta/test/TestThriftParserMore.cpp
    ${MATCHA_SRC}/meta/test/TestThriftMabs.cpp
    ${MATCHA_SRC}/meta/test/TestThriftParserNestedComprehensive.cpp)
target_link_libraries(ThriftParserTests
    PRIVATE surfingdb_core
    PRIVATE ${ARROW_LIBRARY}
    PRIVATE ${PARQUET_LIBRARY}
    PRIVATE GTest::gtest_main
    PRIVATE GTest::gtest)
# Workaround: register the tests explicitly so CTest always finds them
add_test(NAME ThriftParserTests_list COMMAND ThriftParserTests --gtest_list_tests)
add_test(NAME ThriftParserTests_run COMMAND ThriftParserTests)
set_tests_properties(ThriftParserTests_run PROPERTIES WORKING_DIRECTORY ${PROJECT_SOURCE_DIR})

# Parquet folder read/write tests
add_executable(ParquetFolderTests
    ${MATCHA_SRC}/table/test/TestParquetFolder.cpp)
target_link_libraries(ParquetFolderTests
    PRIVATE surfingdb_core
    PRIVATE ${ARROW_LIBRARY}
    PRIVATE ${PARQUET_LIBRARY}
    PRIVATE GTest::gtest_main
    PRIVATE GTest::gtest)
gtest_discover_tests(ParquetFolderTests
  DISCOVERY_MODE PRE_TEST
  PROPERTIES RESOURCE_LOCK "parquet_folder_tests"
)
add_test(NAME ParquetFolderTests_run COMMAND ParquetFolderTests)
set_tests_properties(ParquetFolderTests_run PROPERTIES LABELS "unit;parquet")
# Parquet folder tests share a temp directory; prevent parallel runs from racing.
set_tests_properties(ParquetFolderTests_run PROPERTIES RESOURCE_LOCK "parquet_folder_tests")

# -----------------------------
# Tools: payload generator for JNI decode benchmark
# -----------------------------
add_executable(GenMabsPayloads 
    ${MATCHA_SRC}/meta/tools/gen_mabs_payloads.cpp
    ${MATCHA_SRC}/mabs/mabs_types.cpp
    ${MATCHA_SRC}/mabs/mabs_constants.cpp)
target_link_libraries(GenMabsPayloads PRIVATE ${THRIFT_LINK_LIBS})

# Large payloads generator
add_executable(GenMabsPayloadsLarge
    ${MATCHA_SRC}/meta/tools/gen_mabs_payloads_large.cpp
    ${MATCHA_SRC}/mabs/mabs_types.cpp
    ${MATCHA_SRC}/mabs/mabs_constants.cpp)
target_link_libraries(GenMabsPayloadsLarge PRIVATE ${THRIFT_LINK_LIBS})

# Parquet folder command-line tool
add_executable(parquet_folder_tool
    ${MATCHA_SRC}/table/tools/parquet_folder_tool.cpp)
target_link_libraries(parquet_folder_tool
    PRIVATE surfingdb_core
    PRIVATE ${ARROW_LIBRARY}
    PRIVATE ${PARQUET_LIBRARY}
    PRIVATE ${GLOG_LIBRARY})

# -----------------------------
# MPI Tests (optional)
# -----------------------------
include(CTest)
if(ENABLE_MPI_TESTS)
  find_package(MPI REQUIRED)
  add_executable(MpiShuffleTest ${MATCHA_SRC}/table/test/MpiShuffleRunner.cpp)
  add_dependencies(MpiShuffleTest thrift)
  target_link_libraries(MpiShuffleTest
      PRIVATE surfingdb_core
      PRIVATE ${ARROW_LIBRARY}
      PRIVATE ${PARQUET_LIBRARY})

  if(MPI_CXX_FOUND)
    set(MPIEXEC_NUMPROC_FLAG "-np")
    add_test(NAME MpiShuffleTest
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2 $<TARGET_FILE:MpiShuffleTest>)
  endif()

  add_executable(MpiTwoSideShuffleTest ${MATCHA_SRC}/table/test/MpiTwoSideShuffleRunner.cpp)
  add_dependencies(MpiTwoSideShuffleTest thrift)
  target_link_libraries(MpiTwoSideShuffleTest
      PRIVATE surfingdb_core
      PRIVATE ${ARROW_LIBRARY}
      PRIVATE ${PARQUET_LIBRARY})
  if(MPI_CXX_FOUND)
    add_test(NAME MpiTwoSideShuffleTest_np2
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2 $<TARGET_FILE:MpiTwoSideShuffleTest>)
    add_test(NAME MpiTwoSideShuffleTest_np4
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 $<TARGET_FILE:MpiTwoSideShuffleTest>)
  endif()

  # Randomized distribution tests (one-sided and two-sided)
  add_executable(MpiShuffleRandomTest ${MATCHA_SRC}/table/test/MpiShuffleRandomRunner.cpp)
  target_link_libraries(MpiShuffleRandomTest
      PRIVATE surfingdb_core
      PRIVATE ${ARROW_LIBRARY}
      PRIVATE ${PARQUET_LIBRARY})
  if(MPI_CXX_FOUND)
    add_test(NAME MpiShuffleRandom_one_np4
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 $<TARGET_FILE:MpiShuffleRandomTest>)
    add_test(NAME MpiShuffleRandom_two_np4
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 $<TARGET_FILE:MpiShuffleRandomTest> two)
  endif()

  # Load test (disabled by default in CTest): configurable via env
  add_executable(MpiShuffleLoadTest ${MATCHA_SRC}/table/test/MpiShuffleLoadRunner.cpp)
  target_link_libraries(MpiShuffleLoadTest
      PRIVATE surfingdb_core
      PRIVATE ${ARROW_LIBRARY}
      PRIVATE ${PARQUET_LIBRARY})
  if(MPI_CXX_FOUND)
    # Default to modest rows to avoid OOM in constrained runners; can override via env
    add_test(NAME DISABLED_MpiShuffleLoad_one_np2
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2 $<TARGET_FILE:MpiShuffleLoadTest>)
    add_test(NAME DISABLED_MpiShuffleLoad_two_np2
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2 $<TARGET_FILE:MpiShuffleLoadTest> two)
    set_tests_properties(DISABLED_MpiShuffleLoad_one_np2 PROPERTIES LABELS "load")
    set_tests_properties(DISABLED_MpiShuffleLoad_two_np2 PROPERTIES LABELS "load")
  endif()

  # CoGroup tests (one-sided and two-sided)
  add_executable(MpiCoGroupTest ${MATCHA_SRC}/table/test/MpiCoGroupRunner.cpp)
  target_link_libraries(MpiCoGroupTest
      PRIVATE surfingdb_core
      PRIVATE ${ARROW_LIBRARY}
      PRIVATE ${PARQUET_LIBRARY})
  if(MPI_CXX_FOUND)
    add_test(NAME MpiCoGroup_one_np2
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2 $<TARGET_FILE:MpiCoGroupTest>)
    add_test(NAME MpiCoGroup_two_np2
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2 $<TARGET_FILE:MpiCoGroupTest> two)
  endif()

  # Nested CoGroup tests (one-sided and two-sided)
  add_executable(MpiNestedCoGroupTest ${MATCHA_SRC}/table/test/MpiNestedCoGroupRunner.cpp)
  target_link_libraries(MpiNestedCoGroupTest
      PRIVATE surfingdb_core
      PRIVATE ${ARROW_LIBRARY}
      PRIVATE ${PARQUET_LIBRARY})
  if(MPI_CXX_FOUND)
    add_test(NAME MpiNestedCoGroup_one_np2
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2 $<TARGET_FILE:MpiNestedCoGroupTest>)
    add_test(NAME MpiNestedCoGroup_two_np2
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2 $<TARGET_FILE:MpiNestedCoGroupTest> two)
    add_test(NAME MpiNestedCoGroup_one_np4
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 $<TARGET_FILE:MpiNestedCoGroupTest>)
    add_test(NAME MpiNestedCoGroup_two_np4
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 $<TARGET_FILE:MpiNestedCoGroupTest> two)
    set_tests_properties(MpiNestedCoGroup_one_np2 PROPERTIES LABELS "mpi;nested")
    set_tests_properties(MpiNestedCoGroup_two_np2 PROPERTIES LABELS "mpi;nested")
    set_tests_properties(MpiNestedCoGroup_one_np4 PROPERTIES LABELS "mpi;nested;np4")
    set_tests_properties(MpiNestedCoGroup_two_np4 PROPERTIES LABELS "mpi;nested;np4")
  endif()
  # Nested schema shuffle tests (one-sided and two-sided)
  add_executable(MpiNestedShuffleTest ${MATCHA_SRC}/table/test/MpiNestedShuffleRunner.cpp)
  target_link_libraries(MpiNestedShuffleTest
      PRIVATE surfingdb_core
      PRIVATE ${ARROW_LIBRARY}
      PRIVATE ${PARQUET_LIBRARY})
  if(MPI_CXX_FOUND)
    add_test(NAME MpiNestedShuffle_one_np2
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2 $<TARGET_FILE:MpiNestedShuffleTest>)
    add_test(NAME MpiNestedShuffle_two_np2
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2 $<TARGET_FILE:MpiNestedShuffleTest> two)
    add_test(NAME MpiNestedShuffle_one_np4
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 $<TARGET_FILE:MpiNestedShuffleTest>)
    add_test(NAME MpiNestedShuffle_two_np4
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 $<TARGET_FILE:MpiNestedShuffleTest> two)
    set_tests_properties(MpiNestedShuffle_one_np2 PROPERTIES LABELS "mpi;nested")
    set_tests_properties(MpiNestedShuffle_two_np2 PROPERTIES LABELS "mpi;nested")
    set_tests_properties(MpiNestedShuffle_one_np4 PROPERTIES LABELS "mpi;nested;np4")
    set_tests_properties(MpiNestedShuffle_two_np4 PROPERTIES LABELS "mpi;nested;np4")
  endif()
endif()
# Convenience target to run Thrift->Arrow JNI benchmarks
add_custom_target(thrift_bench
  COMMAND bash ${CMAKE_SOURCE_DIR}/scripts/run_thrift_arrow_bench.sh
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
  COMMENT "Run Thrift->Arrow JNI benchmarks (array, bb, bbp)" VERBATIM)
