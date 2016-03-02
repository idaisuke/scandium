cmake_minimum_required(VERSION 3.3)
project(scandium)

include_directories( BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/include )

find_package(Boost)
IF (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIR})
endif()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
set(CMAKE_EXE_LINKER_FLAGS "-lsqlite3")

set(SOURCE_FILES test_scandium.cpp)
add_executable(scandium ${SOURCE_FILES})
