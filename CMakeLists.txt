cmake_minimum_required(VERSION 3.3)
project(scandium)

include_directories( BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/include )

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(CMAKE_EXE_LINKER_FLAGS "-lsqlite3")

set(SOURCE_FILES main.cpp scandium_test.cpp)
add_executable(scandium ${SOURCE_FILES})