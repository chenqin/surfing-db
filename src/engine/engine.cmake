# define toplogy orchestration
set(ENGINE surfengine)

add_library(${ENGINE} STATIC
        ${SURFINGDB_SRC}/engine/engine.cpp)

target_link_libraries(${ENGINE}
        PRIVATE Threads::Threads
        PRIVATE MPI::MPI_CXX
        PRIVATE ${META}
        PRIVATE ${TABLE}
        PRIVATE ${CONNECTOR}
        PRIVATE ${ARROW_DATASET_LIBRARY}
        PRIVATE ${ARROW_LIBRARY})

# discover all gtests in this module
include(GoogleTest)
gtest_discover_tests(TableTest TEST_LIST ALL)
