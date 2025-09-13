# Require CMake 3.16+ on Mac OSX, as previous versions of CMake had trouble locating
# OpenMP on Mac. See https://github.com/dmlc/xgboost/pull/5146#issuecomment-568312706
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

SET(CMAKE_CUDA_COMPILER "/usr/local/cuda/bin/nvcc")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${TORCH_CXX_FLAGS}")
#list(APPEND CMAKE_PREFIX_PATH "~/libtorch")
#find_package(CUDA REQUIRED 11.8)
#find_package(CUDAToolkit REQUIRED 11.8)
#enable_language(CUDA)

#find_package(Torch REQUIRED)
#find_package(ArrowCUDA REQUIRED)
find_package(ArrowDataset REQUIRED)
set(ARROW_DATASET_LIBRARY ArrowDataset::arrow_dataset_static)
#set(ARROW_CUDA_LIBRARY ArrowCUDA::arrow_cuda_shared)

if(APPLE)
    set(OPENSSL_ROOT_DIR /opt/homebrew/Cellar/openssl@3/3.0.8)
    set(OPENSSL_LIBRARIES ${OPENSSL_ROOT_DIR}/lib)
    set(OPENSSL_INCLUDE_DIR ${OPENSSL_ROOT_DIR}/include)
    set(OPENSSL_CRYPTO_LIBRARY ${OPENSSL_ROOT_DIR}/lib)

    set(BOOST_ROOT_DIR /opt/homebrew/Cellar/boost/1.81.0_1)
    set(BOOST_INCLUDE_DIR /opt/homebrew/Cellar/boost/1.81.0_1/include/)
    include_directories(/opt/homebrew/Cellar/boost/1.81.0_1/include/)
    set(BOOST_LIBRARIES /opt/homebrew/Cellar/boost/1.81.0_1/lib/)

    set(THRIFT_ROOT_DIR /opt/homebrew/Cellar/thrift@0.9/0.9.3.1)
    set(THRIFT_INCLUDE_DIR /opt/homebrew/Cellar/thrift@0.9/0.9.3.1/include)
    set(THRIFT_LIBRARY ${THRIFT_ROOT_DIR}/lib/libthrift.a)

    set(glog_INCLUDE_DIR /opt/homebrew/Cellar/glog/0.6.0/include/)
    set(GLOG_LIBRARY /opt/homebrew/Cellar/glog/0.6.0/lib/libglog.dylib)

    #/opt/homebrew/Cellar/googletest/1.13.0
else()
    #used by thrift
    include(ext/Thrift_Ext.cmake)
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

find_package(OpenMP REQUIRED)
IF (OPENMP_FOUND)
    SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
ENDIF ()
if (NOT TARGET OpenMP::OpenMP_CXX)
    add_library(OpenMP_TARGET INTERFACE)
    add_library(OpenMP::OpenMP_CXX ALIAS OpenMP_TARGET)
    target_compile_options(OpenMP_TARGET INTERFACE ${OpenMP_CXX_FLAGS})
    find_package(Threads REQUIRED)
    target_link_libraries(OpenMP_TARGET INTERFACE Threads::Threads)
    target_link_libraries(OpenMP_TARGET INTERFACE ${OpenMP_CXX_FLAGS})
endif ()
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
include(ext/Gtest_Ext.cmake)
enable_testing()

# add a dependency of glog
#include(ext/Gflags_Ext.cmake)

# add a dependency of glog
#include(ext/Glog_Ext.cmake)

# include utils
#include(ext/Gflatbuffers_Ext.cmake)
include(ext/Json_Ext.cmake)
include(ext/OPENSSL_Ext.cmake)

#set(PYTHON_INCLUDE_DIRS ${PYTHON_INCLUDE_DIRS} /home/chen/.local/lib/python3.10/site-packages/numpy/core/include)
#include_directories(include ${PYTHON_INCLUDE_DIRS})

#arrow
find_package(Arrow REQUIRED)
set(ARROW_LIBRARY Arrow::arrow_static)
#find_package(ArrowCUDA REQUIRED)
find_package(Parquet REQUIRED)
set(PARQUET_LIBRARY Parquet::parquet_static)


find_package(Python3 COMPONENTS Interpreter Development)

# include ML libs
include(ext/Sketches_Ext.cmake)
include(ext/XGBoost_Ext.cmake)
#find_package(lz4)
include(ext/Kafka_Ext.cmake)
set(KAFKA_LIBRARY rdkafka)

# set up directory to search for headers
include_directories(include ${GTEST_INCLUDE_DIRS})
include_directories(include ${GMOCK_INCLUDE_DIRS})

# build table representation
include(src/meta/meta.cmake)

# build table representation
include(src/table/table.cmake)

#include(ext/PyTorch_Ext.cmake)

# build table representation
include(src/connector/connector.cmake)

# build test binary
add_executable(FlinkJobWatcher src/FlinkJobWatcher.cpp)
add_dependencies(FlinkJobWatcher thrift kafka xgboost)

add_executable(LocalCluster ${MATCHA_SRC}/LocalCluster.cpp)

add_executable(MABS 
    ${MATCHA_SRC}/mabs/mabs_types.cpp
    ${MATCHA_SRC}/mabs/mabs_constants.cpp
    ${MATCHA_SRC}/MABS.cpp)
add_dependencies(MABS thrift)

# workaround make issue for now
add_custom_command(TARGET FlinkJobWatcher PRE_BUILD
        COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target kafka
        COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target xgboost
        COMMAND ${CMAKE_COMMAND} --build ${CMAKE_BINARY_DIR} --target thrift)

add_custom_command(TARGET FlinkJobWatcher PRE_BUILD
        COMMAND cd ${CMAKE_SOURCE_DIR}/drsquirrel-java && mvn clean package -Darrow.version=${ARROW_VERSION} 
        COMMAND cp ${CMAKE_SOURCE_DIR}/drsquirrel-java/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar ${CMAKE_SOURCE_DIR}/surfing-db-java.jar)

set_target_properties(MABS PROPERTIES LINK_FLAGS "${MPI_LINK_FLAGS}")

target_link_libraries(MABS
        PRIVATE ${META}
        PRIVATE ${TABLE}
        PRIVATE ${CONNECTOR}
        PRIVATE ${ARROW_LIBRARY}
        PRIVATE ${THRIFT_LIBRARY})

set_target_properties(FlinkJobWatcher PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")

target_link_libraries(FlinkJobWatcher
    PRIVATE ${META}
    PRIVATE ${TABLE}
    PRIVATE ${CONNECTOR}
    PRIVATE ${ARROW_LIBRARY})

set_target_properties(LocalCluster PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}")

target_link_libraries(LocalCluster
    PRIVATE ${META}
    PRIVATE ${TABLE}
    PRIVATE ${CONNECTOR}
    PRIVATE ${ARROW_LIBRARY})

# JNI library to expose KafkaConnector to Java
add_library(surfingkafkajni SHARED ${MATCHA_SRC}/connector/kafka_jni.cpp)
target_link_libraries(surfingkafkajni
    PRIVATE ${META}
    PRIVATE ${TABLE}
    PRIVATE ${CONNECTOR}
    PRIVATE ${ARROW_LIBRARY}
    PRIVATE ${PARQUET_LIBRARY}
    PRIVATE ${GLOG_LIBRARY})
set_target_properties(surfingkafkajni PROPERTIES OUTPUT_NAME surfingkafkajni)

# -----------------------------
# C++ Tests
# -----------------------------
add_executable(SurfingDbTests
    ${MATCHA_SRC}/table/test/TestIntegration.cpp)

add_dependencies(SurfingDbTests gtest thrift kafka xgboost)

target_link_libraries(SurfingDbTests
    PRIVATE ${META}
    PRIVATE ${TABLE}
    PRIVATE ${CONNECTOR}
    PRIVATE ${ARROW_LIBRARY}
    PRIVATE ${PARQUET_LIBRARY}
    PRIVATE ${GTEST_MAIN_LIBRARY}
    PRIVATE ${GTEST_LIBRARY})

add_test(NAME SurfingDbTests COMMAND SurfingDbTests)

# -----------------------------
# MPI Tests (optional)
# -----------------------------
include(CTest)
if(ENABLE_MPI_TESTS)
  find_package(MPI REQUIRED)
  add_executable(MpiShuffleTest ${MATCHA_SRC}/table/test/MpiShuffleRunner.cpp)
  add_dependencies(MpiShuffleTest thrift xgboost)
  target_link_libraries(MpiShuffleTest
      PRIVATE ${TABLE}
      PRIVATE ${META}
      PRIVATE ${ARROW_LIBRARY}
      PRIVATE ${PARQUET_LIBRARY})

  if(MPI_CXX_FOUND)
    set(MPIEXEC_NUMPROC_FLAG "-np")
    add_test(NAME MpiShuffleTest
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2 $<TARGET_FILE:MpiShuffleTest>)
  endif()

  add_executable(MpiTwoSideShuffleTest ${MATCHA_SRC}/table/test/MpiTwoSideShuffleRunner.cpp)
  add_dependencies(MpiTwoSideShuffleTest thrift xgboost)
  target_link_libraries(MpiTwoSideShuffleTest
      PRIVATE ${TABLE}
      PRIVATE ${META}
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
      PRIVATE ${TABLE}
      PRIVATE ${META}
      PRIVATE ${ARROW_LIBRARY}
      PRIVATE ${PARQUET_LIBRARY})
  if(MPI_CXX_FOUND)
    add_test(NAME MpiShuffleRandom_one_np4
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 $<TARGET_FILE:MpiShuffleRandomTest>)
    add_test(NAME MpiShuffleRandom_two_np4
      COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 4 $<TARGET_FILE:MpiShuffleRandomTest> two)
  endif()
endif()
