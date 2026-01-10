cmake_minimum_required(VERSION 3.16)
project(kvstore C)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_C_STANDARD 99)

# Let CMake locate JNI headers; no hardcoded JAVA_HOME required.
find_package(JNI REQUIRED)

add_library(kvstore SHARED
    kvstore.c
)

target_include_directories(kvstore PRIVATE
    ${JNI_INCLUDE_DIRS}
)

