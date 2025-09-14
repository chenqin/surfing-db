cmake_minimum_required(VERSION 3.18)
project(surfingdb_cuda_tests LANGUAGES CXX CUDA)

# Explicitly enable CXX to ensure compile features are detected
enable_language(CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CUDA_STANDARD 14)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)

enable_testing()

find_package(CUDAToolkit REQUIRED)
find_package(MPI REQUIRED)

add_library(surfingcuda STATIC cuda_utils.cu)
target_include_directories(surfingcuda PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(surfingcuda PRIVATE CUDA::cudart)

add_executable(CudaTests test/TestCuda.cpp)
target_include_directories(CudaTests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(CudaTests PRIVATE surfingcuda)

add_test(NAME CudaTests COMMAND CudaTests)

# MPI-aware CUDA test
add_executable(CudaMpiTests test/TestCudaMpi.cpp)
target_include_directories(CudaMpiTests PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/..)
target_link_libraries(CudaMpiTests PRIVATE surfingcuda MPI::MPI_CXX)

# Use CMake's discovered MPI launcher and numproc flag
if (MPIEXEC_EXECUTABLE)
  if (NOT DEFINED MPIEXEC_NUMPROC_FLAG)
    set(MPIEXEC_NUMPROC_FLAG -np)
  endif()
  add_test(NAME CudaMpiTests_np2
           COMMAND ${MPIEXEC_EXECUTABLE} ${MPIEXEC_NUMPROC_FLAG} 2 $<TARGET_FILE:CudaMpiTests>)
endif()
