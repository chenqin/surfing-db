# define toplogy orchestration
set(ENGINE surfengine)

add_library(${ENGINE} 
        ${SURFINGDB_SRC}/engine/engine.cpp)

target_link_libraries(${ENGINE}
        PRIVATE Threads::Threads
        PRIVATE MPI::MPI_CXX
        PRIVATE ${META}
        PRIVATE ${TABLE}
        PRIVATE ${CONNECTOR}
        PRIVATE ${ARROW_DATASET_LIBRARY}
        PRIVATE ${ARROW_LIBRARY})

        # build test binary
add_executable(EngineTest
        ${SURFINGDB_SRC}/engine/test/TestIntegration.cpp)

target_link_libraries(EngineTest
        PRIVATE Threads::Threads
        PRIVATE MPI::MPI_CXX
        PRIVATE ${ENGINE}
        PRIVATE ${GTEST_LIBRARY}
        PRIVATE ${GTEST_MAIN_LIBRARY}
        PRIVATE ${META}
        PRIVATE ${TABLE}
        PRIVATE ${CONNECTOR}
        PRIVATE ${ARROW_DATASET_LIBRARY}
        PRIVATE ${ARROW_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(EngineTest TEST_LIST ALL)
