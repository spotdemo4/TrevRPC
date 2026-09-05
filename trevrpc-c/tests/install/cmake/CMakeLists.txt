cmake_minimum_required(VERSION 3.20)
project(trevrpc-installed-cmake-consumer LANGUAGES C CXX)

option(TREVRPC_INSTALL_TEST_ENGINE_ONLY "Build only the installed engine consumer" OFF)
option(TREVRPC_INSTALL_TEST_ENGINE_MSQUIC_ONLY "Build only the installed Engine consumers" OFF)
option(TREVRPC_INSTALL_TEST_RPC_MSQUIC_ONLY "Build only the installed Engine and RPC consumers" OFF)

find_package(trevrpc_engine CONFIG REQUIRED)
if(NOT TREVRPC_ENGINE_ABI_VERSION EQUAL 1)
    message(FATAL_ERROR "installed TrevRPC engine package does not expose Engine ABI 1")
endif()
add_executable(trevrpc-installed-engine-cmake-consumer engine_main.c)
target_link_libraries(trevrpc-installed-engine-cmake-consumer PRIVATE trevrpc::trevrpc_engine)

if(TREVRPC_INSTALL_TEST_ENGINE_ONLY)
    return()
endif()

find_package(trevrpc_engine_msquic CONFIG REQUIRED)
if(NOT TREVRPC_ENGINE_MSQUIC_ABI_VERSION EQUAL 1)
    message(FATAL_ERROR "installed TrevRPC MsQuic engine package does not expose provider ABI 1")
endif()
add_executable(trevrpc-installed-engine-msquic-cmake-consumer engine_msquic_main.c)
target_link_libraries(trevrpc-installed-engine-msquic-cmake-consumer PRIVATE
    trevrpc::trevrpc_engine_msquic
)
add_executable(trevrpc-installed-engine-msquic-cmake-cpp-consumer engine_msquic_main.cpp)
target_link_libraries(trevrpc-installed-engine-msquic-cmake-cpp-consumer PRIVATE
    trevrpc::trevrpc_engine_msquic
)

if(TREVRPC_INSTALL_TEST_ENGINE_MSQUIC_ONLY)
    return()
endif()

find_package(trevrpc_rpc CONFIG REQUIRED)
if(NOT TREVRPC_RPC_ABI_VERSION EQUAL 1)
    message(FATAL_ERROR "installed TrevRPC RPC package does not expose RPC ABI 1")
endif()
add_executable(trevrpc-installed-rpc-cmake-consumer rpc_main.c)
target_link_libraries(trevrpc-installed-rpc-cmake-consumer PRIVATE trevrpc::trevrpc_rpc)

find_package(trevrpc_rpc_msquic CONFIG REQUIRED)
if(NOT TREVRPC_RPC_MSQUIC_ABI_VERSION EQUAL 1)
    message(FATAL_ERROR "installed TrevRPC RPC MsQuic package does not expose companion ABI 1")
endif()
add_executable(trevrpc-installed-rpc-msquic-cmake-consumer rpc_msquic_main.c)
target_link_libraries(trevrpc-installed-rpc-msquic-cmake-consumer PRIVATE trevrpc::trevrpc_rpc_msquic)

if(TREVRPC_INSTALL_TEST_RPC_MSQUIC_ONLY)
    return()
endif()

find_package(PkgConfig REQUIRED)
pkg_check_modules(PROTOBUF_C REQUIRED IMPORTED_TARGET libprotobuf-c)
find_program(PROTOC_EXECUTABLE protoc REQUIRED)
find_program(PROTOC_C_EXECUTABLE protoc-c REQUIRED)
find_program(TREVRPC_C_GENERATOR_EXECUTABLE protoc-gen-trevrpc-c REQUIRED)

set(proto_dir "${CMAKE_CURRENT_LIST_DIR}/../codegen")
set(generated_dir "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(generated_files
    "${generated_dir}/greeter.pb-c.c"
    "${generated_dir}/greeter.pb-c.h"
    "${generated_dir}/greeter.trevrpc.c"
    "${generated_dir}/greeter.trevrpc.h"
)
add_custom_command(
    OUTPUT ${generated_files}
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${generated_dir}"
    COMMAND "${PROTOC_C_EXECUTABLE}" -I "${proto_dir}" --c_out=${generated_dir} "${proto_dir}/greeter.proto"
    COMMAND "${PROTOC_EXECUTABLE}" -I "${proto_dir}"
        --plugin=protoc-gen-trevrpc-c=${TREVRPC_C_GENERATOR_EXECUTABLE}
        --trevrpc-c_out=${generated_dir} "${proto_dir}/greeter.proto"
    DEPENDS "${proto_dir}/greeter.proto" "${TREVRPC_C_GENERATOR_EXECUTABLE}"
    VERBATIM
)

add_executable(trevrpc-installed-cmake-consumer
    "${CMAKE_CURRENT_LIST_DIR}/../codegen/main.c"
    "${generated_dir}/greeter.pb-c.c"
    "${generated_dir}/greeter.trevrpc.c"
)
target_include_directories(trevrpc-installed-cmake-consumer PRIVATE "${generated_dir}")
target_link_libraries(trevrpc-installed-cmake-consumer PRIVATE trevrpc::trevrpc_rpc PkgConfig::PROTOBUF_C)
