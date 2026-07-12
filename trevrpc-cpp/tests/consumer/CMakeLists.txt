cmake_minimum_required(VERSION 3.20)

project(trevrpc-cpp-consumer LANGUAGES CXX)

find_package(trevrpc-cpp CONFIG REQUIRED)

trevrpc_cpp_generate(
  TARGET trevrpc_cpp_consumer_generated
  PROTO "${CMAKE_CURRENT_SOURCE_DIR}/proto/nested/service.proto"
  IMPORT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto"
  OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")

add_executable(trevrpc_cpp_consumer main.cpp)
target_compile_features(trevrpc_cpp_consumer PRIVATE cxx_std_20)
target_link_libraries(trevrpc_cpp_consumer PRIVATE trevrpc_cpp_consumer_generated)
