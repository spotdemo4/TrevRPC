cmake_minimum_required(VERSION 3.20)
project(trevrpc-installed-cmake-consumer LANGUAGES C)

find_package(trevrpc 0.2 CONFIG REQUIRED)
if(NOT TREVRPC_C_ABI_VERSION EQUAL 6)
    message(FATAL_ERROR "installed TrevRPC package does not expose C ABI 6")
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
target_link_libraries(trevrpc-installed-cmake-consumer PRIVATE trevrpc::trevrpc PkgConfig::PROTOBUF_C)
