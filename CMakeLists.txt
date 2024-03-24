cmake_minimum_required(VERSION 3.16)

project(ArrowTesting)
# do not optimize on debug
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
# march tells the compiler to use the native isa of this pcs cpus,
# which includes using simd extensions e.g. sse 
set(CMAKE_CXX_FLAGS_RELEASE "-O3 -march=native")
set(CMAKE_VERBOSE_MAKEFILE TRUE)

option(BUILD_EVAL "building arrow_eval" OFF)

if(BUILD_EVAL)
    add_executable(arrow_eval arrow_eval.cpp)
    #link to arrow pkg:
    find_package(Arrow REQUIRED)
    target_link_libraries(arrow_eval PRIVATE Arrow::arrow_shared)
else()
    find_package(Arrow REQUIRED)
    find_package(Gandiva REQUIRED)
    find_package(Parquet REQUIRED)
    find_package(ArrowDataset REQUIRED)
    add_executable(arrow_testing arrow_testing.cpp) # compile and make exe
    target_link_libraries(arrow_testing PRIVATE Arrow::arrow_shared)
    target_link_libraries(arrow_testing PRIVATE Parquet::parquet_shared) # link found parquet lib
    target_link_libraries(arrow_testing PRIVATE ArrowDataset::arrow_dataset_shared) # link found arrow dataset lib
    target_link_libraries(arrow_testing PRIVATE Gandiva::gandiva_shared)
endif()
