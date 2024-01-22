cmake_minimum_required(VERSION 3.16)

project(ArrowTesting)
# do not optimize on debug
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
# march tells the compiler to use the native isa of this pcs cpus,
# which includes using simd extensions e.g. sse 
# higher optimization level may lead to optimized custom fct
set(CMAKE_CXX_FLAGS_RELEASE "-O1 -march=native")
#set(CMAKE_VERBOSE_MAKEFILE TRUE)

option(BUILD_EVAL "building arrow_eval" OFF)

find_package(Arrow REQUIRED)

if(BUILD_EVAL)
    add_executable(arrow_eval arrow_eval.cpp) # compile and make exe
    target_link_libraries(arrow_eval PRIVATE Arrow::arrow_shared) # link found arrow lib
else()
    find_package(Gandiva REQUIRED)
    find_package(Parquet REQUIRED)
    find_package(ArrowDataset REQUIRED)
    add_executable(arrow_testing arrow_testing.cpp) # compile and make exe
    target_link_libraries(arrow_testing PRIVATE Parquet::parquet_shared) # link found parquet lib
    target_link_libraries(arrow_testing PRIVATE ArrowDataset::arrow_dataset_shared) # link found arrow dataset lib
    target_link_libraries(arrow_testing PRIVATE Gandiva::gandiva_shared)
endif()
