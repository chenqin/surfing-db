# Require CMake 3.16+ on Mac OSX, as previous versions of CMake had trouble locating
# OpenMP on Mac. See https://github.com/dmlc/xgboost/pull/5146#issuecomment-568312706
cmake_minimum_required(VERSION 3.16)
project(surfingdb LANGUAGES CXX VERSION 0.1)


if (linux)
    project(Treads)
    #set( CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -std=gnu++2a -pthread" )
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
    set(THREADS_PREFER_PTHREAD_FLAG TRUE)
endif ()

find_package(Threads REQUIRED)
find_package(MPI REQUIRED)
find_package(OpenMP REQUIRED)
find_package(OpenSSL REQUIRED)
#https://pybind11.readthedocs.io/en/stable/advanced/embedding.html
find_package(pybind11 REQUIRED)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_VERBOSE_MAKEFILE ON)

set(SURFINGDB_ROOT ${PROJECT_SOURCE_DIR})
set(SURFINGDB_SRC ${SURFINGDB_ROOT}/src)

SET(CMAKE_C_FLAGS ${CMAKE_C_FLAGS} -fopenmp)

include_directories(include ${SURFINGDB_SRC} ${MPI_CXX_INCLUDE_PATH} ${MPI_INCLUDE_PATH})

# a switch to use clang or g++
# clang or g++
# this switch only works on linux
set(NC gcc)
if (APPLE)
    set(NCXX /usr/bin/g++)
    set(NCC /usr/bin/gcc)
    set(NCFLAGS "-Wall -Wextra -Werror -Wno-error=extern-c-compat -Wno-error=inconsistent-missing-override -Wno-error=nullability-completeness -Wno-error=sign-compare -Wno-error=unknown-warning-option")
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

# use openmp simd
OPTION(USE_OpenMP "Use OpenMP" ON)

IF (USE_OpenMP)
    message(STATUS "USE_OpenMP=${USE_OpenMP}")
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
ENDIF ()


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

include(ext/FMT_Ext.cmake)

include(ext/PGQuery_Ext.cmake)

# add a basic gtest dependency
include(ext/Gtest_Ext.cmake)

# add a dependency of glog
include(ext/Gflags_Ext.cmake)

# add a dependency of glog
find_package (glog REQUIRED)

# include utils
include(ext/Gflatbuffers_Ext.cmake)
include(ext/Json_Ext.cmake)
include(ext/OPENSSL_Ext.cmake)

set(PYTHON_INCLUDE_DIRS ${PYTHON_INCLUDE_DIRS} /home/chen/.local/lib/python3.10/site-packages/numpy/core/include)
include_directories(include ${PYTHON_INCLUDE_DIRS})

#used by thrift
find_package(Boost REQUIRED)
include(ext/Thrift_Ext.cmake)
#find_package(Thrift REQUIRED)
#include(ext/Arrow_Ext.cmake)
#include(ext/Arrow.cmake)
#build_arrow()
find_package(Arrow REQUIRED)
set(ARROW_LIBRARY Arrow::arrow_shared)
find_package(Parquet REQUIRED)
set(PARQUET_LIBRARY Parquet::parquet_shared)

find_package(Python3 COMPONENTS Interpreter Development)

# include ML libs
include(ext/Sketches_Ext.cmake)
include(ext/XGBoost_Ext.cmake)

include(ext/Kafka_Ext.cmake)

# set up directory to search for headers
include_directories(include ${GTEST_INCLUDE_DIRS})
include_directories(include ${GMOCK_INCLUDE_DIRS})


# build sql parser
include(src/parser/parser.cmake)

# build table representation
include(src/meta/meta.cmake)

# build table representation
include(src/table/table.cmake)

# build table representation
include(src/connector/connector.cmake)
# build test binary
add_executable(MainTest src/main.cpp)
add_executable(Test src/TestIntegration.cpp)

target_link_libraries(MainTest
        PRIVATE ${META}
        PRIVATE ${TABLE}
        PRIVATE ${CONNECTOR}
        PRIVATE ${ARROW_LIBRARY}
        PRIVATE ${Python3_LIBRARIES})

target_link_libraries(Test
        PRIVATE ${META}
        PRIVATE ${TABLE}
        PRIVATE ${CONNECTOR}
        PRIVATE ${ARROW_LIBRARY}
        PRIVATE ${Python3_LIBRARIES})