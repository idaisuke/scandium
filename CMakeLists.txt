cmake_minimum_required(VERSION 3.3)
project(scandium)

include_directories(BEFORE ${CMAKE_CURRENT_SOURCE_DIR}/include)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES main.cpp test_scandium.cpp)
add_executable(test_scandium ${SOURCE_FILES})
target_link_libraries(test_scandium sqlite3)

#add_definitions(-DSQLITE_HAS_CODEC)
#target_link_libraries(test_scandium crypto /usr/local/lib/libsqlcipher.a)

#set(Boost_USE_STATIC_LIBS OFF)
set(Boost_USE_MULTITHREAD ON)
find_package(Boost)
IF (Boost_FOUND)
    include_directories(${Boost_INCLUDE_DIR})
    target_link_libraries(test_scandium ${Boost_LIBRARIES})
endif ()
