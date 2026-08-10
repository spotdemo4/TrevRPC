cmake_minimum_required(VERSION 3.20)

project(trevrpc-js-native LANGUAGES C)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

option(TREVRPC_NODE_TEST_HOOKS "Build test-only Node addon debug hooks" OFF)

get_filename_component(trevrpc_c_sibling "${CMAKE_CURRENT_LIST_DIR}/../../trevrpc-c" ABSOLUTE)
if(EXISTS "${trevrpc_c_sibling}/CMakeLists.txt")
    set(trevrpc_c_default_root "${trevrpc_c_sibling}")
else()
    set(trevrpc_c_default_root "")
endif()
set(TREVRPC_C_ROOT "${trevrpc_c_default_root}" CACHE PATH
    "Optional path to a trevrpc-c source tree; empty uses an installed package")

if(TREVRPC_C_ROOT)
    if(NOT EXISTS "${TREVRPC_C_ROOT}/CMakeLists.txt")
        message(FATAL_ERROR "TREVRPC_C_ROOT does not point to trevrpc-c: ${TREVRPC_C_ROOT}")
    endif()
    set(TREVRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
    set(TREVRPC_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
    set(TREVRPC_BUILD_MSQUIC ON CACHE BOOL "" FORCE)
    set(TREVRPC_BUILD_WEBTRANSPORT ON CACHE BOOL "" FORCE)
    set(TREVRPC_BUILD_RUNTIME ON CACHE BOOL "" FORCE)
    add_subdirectory("${TREVRPC_C_ROOT}" "${CMAKE_CURRENT_BINARY_DIR}/trevrpc-c" EXCLUDE_FROM_ALL)
    set(trevrpc_node_c_target trevrpc)
else()
    find_package(trevrpc CONFIG REQUIRED)
    set(trevrpc_node_c_target trevrpc::trevrpc)
endif()

if(NOT DEFINED NODE_INCLUDE_DIR)
    execute_process(
        COMMAND node -p "require('node:path').join(process.execPath, '..', '..', 'include', 'node')"
        OUTPUT_VARIABLE NODE_INCLUDE_DIR
        OUTPUT_STRIP_TRAILING_WHITESPACE
    )
endif()

if(NOT EXISTS "${NODE_INCLUDE_DIR}/node_api.h")
    message(FATAL_ERROR "NODE_INCLUDE_DIR does not contain node_api.h: ${NODE_INCLUDE_DIR}")
endif()

add_library(trevrpc_native MODULE
    trevrpc_node.c
)
target_include_directories(trevrpc_native PRIVATE "${NODE_INCLUDE_DIR}")
target_link_libraries(trevrpc_native PRIVATE ${trevrpc_node_c_target})
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    target_link_options(trevrpc_native PRIVATE "LINKER:-undefined,dynamic_lookup")
endif()
if(TREVRPC_NODE_TEST_HOOKS)
    target_compile_definitions(trevrpc_native PRIVATE TREVRPC_NODE_TEST_HOOKS=1)
endif()
set_target_properties(trevrpc_native PROPERTIES
    PREFIX ""
    SUFFIX ".node"
    LIBRARY_OUTPUT_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}"
)

if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(trevrpc_native PRIVATE -Wall -Wextra -Werror)
endif()
