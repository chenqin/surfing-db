# Build the Arrow C++ libraries.
function(build_arrow)
    set(one_value_args)
    set(multi_value_args)

    cmake_parse_arguments(ARG
            "${options}"
            "${one_value_args}"
            "${multi_value_args}"
            ${ARGN})
    if (ARG_UNPARSED_ARGUMENTS)
        message(SEND_ERROR "Error: unrecognized arguments: ${ARG_UNPARSED_ARGUMENTS}")
    endif ()

    # If Arrow needs to be built, the default location will be within the build tree.
    set(ARROW_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-prefix")

    set(ARROW_SHARED_LIBRARY_DIR "${ARROW_PREFIX}/lib")

    set(ARROW_SHARED_LIB_FILENAME
            "${CMAKE_SHARED_LIBRARY_PREFIX}arrow${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(ARROW_SHARED_LIB "${ARROW_SHARED_LIBRARY_DIR}/${ARROW_SHARED_LIB_FILENAME}")
    set(PARQUET_SHARED_LIB_FILENAME
            "${CMAKE_SHARED_LIBRARY_PREFIX}parquet${CMAKE_SHARED_LIBRARY_SUFFIX}")
    set(PARQUET_SHARED_LIB "${ARROW_SHARED_LIBRARY_DIR}/${PARQUET_SHARED_LIB_FILENAME}")

    set(ARROW_BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/arrow_ep-build")
    set(ARROW_CMAKE_ARGS "-DCMAKE_INSTALL_PREFIX=${ARROW_PREFIX}"
            "-DCMAKE_INSTALL_LIBDIR=lib" "-Dxsimd_SOURCE=BUNDLED" "-DARROW_EXTRA_ERROR_CONTEXT=ON"
            "-DARROW_BUILD_STATIC=OFF" "-DARROW_PARQUET=ON" "-DARROW_COMPUTE=ON"
            "-DARROW_WITH_UTF8PROC=OFF" "-DARROW_WITH_RE2=OFF" "-DThrift_SOURCE=BUNDLED"
            "-DARROW_FILESYSTEM=ON" "-DARROW_CSV=ON" "-DARROW_PYTHON=ON")
    set(ARROW_INCLUDE_DIR "${ARROW_PREFIX}/include")

    set(ARROW_BUILD_BYPRODUCTS "${ARROW_SHARED_LIB}" "${PARQUET_SHARED_LIB}")

    include(ExternalProject)

    externalproject_add(arrow_ep
            GIT_REPOSITORY https://github.com/apache/arrow.git
            GIT_TAG apache-arrow-11.0.0
            SOURCE_SUBDIR cpp
            BINARY_DIR "${ARROW_BINARY_DIR}"
            CMAKE_ARGS "${ARROW_CMAKE_ARGS}"
            BUILD_BYPRODUCTS "${ARROW_BUILD_BYPRODUCTS}")

    set(ARROW_LIBRARY arrow_shared)
    set(PARQUET_LIBRARY parquet_shared)

    file(MAKE_DIRECTORY "${ARROW_INCLUDE_DIR}")
    add_library(${ARROW_LIBRARY} SHARED IMPORTED)
    add_library(${PARQUET_LIBRARY} SHARED IMPORTED)
    set_target_properties(${ARROW_LIBRARY}
            PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR}
            IMPORTED_LOCATION ${ARROW_SHARED_LIB})
    set_target_properties(${PARQUET_LIBRARY}
            PROPERTIES INTERFACE_INCLUDE_DIRECTORIES ${ARROW_INCLUDE_DIR}
            IMPORTED_LOCATION ${PARQUET_SHARED_LIB})

    add_dependencies(${ARROW_LIBRARY} arrow_ep)

    message(STATUS "arrow_include=${ARROW_INCLUDE_DIR}")
    include_directories(include ${ARROW_INCLUDE_DIR})
endfunction()