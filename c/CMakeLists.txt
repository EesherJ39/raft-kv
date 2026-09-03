cmake_minimum_required(VERSION 3.16)
project(kvstore C)

set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)

# Only the JVM component is needed; this keeps headless CI images sufficient.
find_package(JNI REQUIRED COMPONENTS JVM)
find_package(Threads REQUIRED)

add_library(kvstore SHARED
    kvstore.c
)

target_include_directories(kvstore PRIVATE
    ${JNI_INCLUDE_DIRS}
)

target_link_libraries(kvstore PRIVATE Threads::Threads)
target_compile_options(kvstore PRIVATE -Wall -Wextra -Wpedantic -Werror)

