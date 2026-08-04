cmake_minimum_required(VERSION 3.20)

project(trevrpc-conformance-c-family LANGUAGES C CXX)

include(GNUInstallDirs)
include(CTest)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

get_filename_component(TREVRPC_REPO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../../.." ABSOLUTE)
set(TREVRPC_C_FAMILY_C_ROOT "${TREVRPC_REPO_ROOT}/trevrpc-c" CACHE PATH
    "Path to the trevrpc-c source tree")
set(TREVRPC_C_FAMILY_CPP_ROOT "${TREVRPC_REPO_ROOT}/trevrpc-cpp" CACHE PATH
    "Path to the trevrpc-cpp source tree")
option(TREVRPC_C_FAMILY_ENABLE_SANITIZERS "Enable ASan and UBSan" OFF)

function(cf_c_warnings target)
  if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(${target} PRIVATE -Wall -Wextra -Werror)
  endif()
endfunction()

function(cf_cpp_warnings target)
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic -Werror)
  endif()
endfunction()

function(cf_sanitizers target)
  if(TREVRPC_C_FAMILY_ENABLE_SANITIZERS AND CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(${target} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(${target} PRIVATE -fsanitize=address,undefined)
  endif()
endfunction()

set(TREVRPC_ENABLE_SANITIZERS "${TREVRPC_C_FAMILY_ENABLE_SANITIZERS}" CACHE BOOL "" FORCE)
set(TREVRPC_CPP_ENABLE_SANITIZERS "${TREVRPC_C_FAMILY_ENABLE_SANITIZERS}" CACHE BOOL "" FORCE)
set(TREVRPC_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(TREVRPC_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(TREVRPC_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(TREVRPC_BUILD_CODEGEN OFF CACHE BOOL "" FORCE)
set(TREVRPC_BUILD_RUNTIME ON CACHE BOOL "" FORCE)
add_subdirectory("${TREVRPC_C_FAMILY_C_ROOT}" "${CMAKE_CURRENT_BINARY_DIR}/trevrpc-c")

set(TREVRPC_CPP_BUILD_CODEGEN OFF CACHE BOOL "" FORCE)
set(TREVRPC_CPP_BUILD_TESTS OFF CACHE BOOL "" FORCE)
set(TREVRPC_CPP_BUILD_EXAMPLES OFF CACHE BOOL "" FORCE)
set(TREVRPC_CPP_BUILD_BENCHMARKS OFF CACHE BOOL "" FORCE)
set(TREVRPC_CPP_TREVRPC_C_ROOT "" CACHE PATH "" FORCE)
add_subdirectory("${TREVRPC_C_FAMILY_CPP_ROOT}" "${CMAKE_CURRENT_BINARY_DIR}/trevrpc-cpp")

find_package(Protobuf CONFIG REQUIRED)
set(cf_generated_dir "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(cf_state_proto_cc "${cf_generated_dir}/state_payload.pb.cc")
set(cf_state_proto_h "${cf_generated_dir}/state_payload.pb.h")
add_custom_command(
  OUTPUT "${cf_state_proto_cc}" "${cf_state_proto_h}"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${cf_generated_dir}"
  COMMAND protobuf::protoc
    -I "${CMAKE_CURRENT_SOURCE_DIR}"
    --cpp_out=${cf_generated_dir}
    "${CMAKE_CURRENT_SOURCE_DIR}/state_payload.proto"
  DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/state_payload.proto"
  VERBATIM)
add_library(cf_state_payload STATIC "${cf_state_proto_cc}")
target_include_directories(cf_state_payload PUBLIC "${cf_generated_dir}")
target_link_libraries(cf_state_payload PUBLIC protobuf::libprotobuf)
cf_cpp_warnings(cf_state_payload)
cf_sanitizers(cf_state_payload)

add_library(cf_common STATIC protocol.c operations.c)
target_include_directories(cf_common
  PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}"
  PRIVATE "${TREVRPC_C_FAMILY_C_ROOT}/src")
target_link_libraries(cf_common PUBLIC trevrpc trevrpc_core)
cf_c_warnings(cf_common)
cf_sanitizers(cf_common)

add_executable(trevrpc_conformance_c c_main.c c_state.c)
set_target_properties(trevrpc_conformance_c PROPERTIES OUTPUT_NAME "trevrpc-conformance-c")
target_include_directories(trevrpc_conformance_c PRIVATE "${TREVRPC_C_FAMILY_C_ROOT}/src")
target_link_libraries(trevrpc_conformance_c PRIVATE cf_common)
cf_c_warnings(trevrpc_conformance_c)
cf_sanitizers(trevrpc_conformance_c)

add_executable(trevrpc_conformance_cpp cpp_main.cpp cpp_state.cpp)
set_target_properties(trevrpc_conformance_cpp PROPERTIES OUTPUT_NAME "trevrpc-conformance-cpp")
target_include_directories(trevrpc_conformance_cpp PRIVATE
  "${TREVRPC_C_FAMILY_C_ROOT}/src"
  "${cf_generated_dir}")
target_link_libraries(trevrpc_conformance_cpp PRIVATE
  cf_common
  cf_state_payload
  trevrpc_cpp)
cf_cpp_warnings(trevrpc_conformance_cpp)
cf_sanitizers(trevrpc_conformance_cpp)

install(TARGETS trevrpc_conformance_c trevrpc_conformance_cpp
  RUNTIME DESTINATION "${CMAKE_INSTALL_BINDIR}")

if(BUILD_TESTING)
  find_package(Python3 REQUIRED COMPONENTS Interpreter)
  add_test(NAME trevrpc_conformance_c_process
    COMMAND Python3::Interpreter "${CMAKE_CURRENT_SOURCE_DIR}/tests/process_test.py"
      "$<TARGET_FILE:trevrpc_conformance_c>" c)
  add_test(NAME trevrpc_conformance_cpp_process
    COMMAND Python3::Interpreter "${CMAKE_CURRENT_SOURCE_DIR}/tests/process_test.py"
      "$<TARGET_FILE:trevrpc_conformance_cpp>" cpp)

  add_executable(trevrpc_conformance_cpp_stream_test tests/cpp_stream_test.cpp)
  target_include_directories(trevrpc_conformance_cpp_stream_test PRIVATE
    "${TREVRPC_C_FAMILY_C_ROOT}/src"
    "${cf_generated_dir}")
  target_link_libraries(trevrpc_conformance_cpp_stream_test PRIVATE
    cf_state_payload
    trevrpc_cpp)
  cf_cpp_warnings(trevrpc_conformance_cpp_stream_test)
  cf_sanitizers(trevrpc_conformance_cpp_stream_test)
  add_test(NAME trevrpc_conformance_cpp_stream
    COMMAND trevrpc_conformance_cpp_stream_test)

  add_executable(trevrpc_conformance_protocol_allocation_test
    tests/protocol_allocation_test.c)
  target_include_directories(trevrpc_conformance_protocol_allocation_test PRIVATE
    "${CMAKE_CURRENT_SOURCE_DIR}"
    "${TREVRPC_C_FAMILY_C_ROOT}/include")
  cf_c_warnings(trevrpc_conformance_protocol_allocation_test)
  cf_sanitizers(trevrpc_conformance_protocol_allocation_test)
  add_test(NAME trevrpc_conformance_protocol_allocation
    COMMAND trevrpc_conformance_protocol_allocation_test)

  set_tests_properties(
    trevrpc_conformance_c_process
    trevrpc_conformance_cpp_process
    trevrpc_conformance_cpp_stream
    trevrpc_conformance_protocol_allocation
    PROPERTIES TIMEOUT 30)
endif()
