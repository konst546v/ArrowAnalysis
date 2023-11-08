cmake_minimum_required(VERSION 3.16)

project(ArrowTesting)
find_package(Arrow REQUIRED)
# ${Arrow_FOUND} true if lib has been found
# ${ARROW_VERSION} contains arrow version
# ${ARROW_FULL_SO_VERSION} dll version
# CMAKE_PREFIX_PATH = <dirOfArrowInstall> # uncomment if cant be found
add_executable(arrow_testing arrow_testing.cpp) # compile and make exe
target_link_libraries(arrow_testing PRIVATE Arrow::arrow_shared) # link found arrow lib