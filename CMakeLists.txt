cmake_minimum_required(VERSION 3.16)

project(ArrowTesting)
# do not optimize on debug, but use high optimisation otherwise
set(CMAKE_CXX_FLAGS_DEBUG "-g -O0")
set(CMAKE_CXX_FLAGS_RELEASE "-O3")
find_package(Arrow REQUIRED)
# the apache arrow pkg
find_package(Parquet REQUIRED)
# the pkg for writing and parsing .parquet files
find_package(ArrowDataset REQUIRED)
# the pkg for reading and writing datasets
# more info: https://arrow.apache.org/docs/cpp/build_system.html
# ${Arrow_FOUND} true if lib has been found
# ${ARROW_VERSION} contains arrow version
# ${ARROW_FULL_SO_VERSION} dll version
# CMAKE_PREFIX_PATH = <dirOfArrowInstall> # uncomment if cant be found
add_executable(arrow_testing arrow_testing.cpp) # compile and make exe
target_link_libraries(arrow_testing PRIVATE Arrow::arrow_shared) # link found arrow lib
target_link_libraries(arrow_testing PRIVATE Parquet::parquet_shared) # link found parquet lib
target_link_libraries(arrow_testing PRIVATE ArrowDataset::arrow_dataset_shared) # link found arrow dataset lib