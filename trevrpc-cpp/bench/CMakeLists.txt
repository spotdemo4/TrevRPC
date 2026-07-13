set(trevrpc_cpp_benchmark_proto
    "${CMAKE_CURRENT_SOURCE_DIR}/../../bench/proto/benchmark.proto")
if(NOT EXISTS "${trevrpc_cpp_benchmark_proto}")
  message(FATAL_ERROR "The canonical benchmark proto was not found: ${trevrpc_cpp_benchmark_proto}")
endif()

trevrpc_cpp_generate(
  TARGET trevrpc_cpp_benchmark_generated
  PROTO "${trevrpc_cpp_benchmark_proto}"
  IMPORT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../../bench/proto"
  OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")

find_package(Threads REQUIRED)
add_executable(trevrpc_cpp_bench_peer trevrpc_bench_peer.cpp)
set_target_properties(trevrpc_cpp_bench_peer PROPERTIES OUTPUT_NAME trevrpc-bench-peer-cpp)
target_link_libraries(trevrpc_cpp_bench_peer PRIVATE
  trevrpc_cpp_benchmark_generated
  Threads::Threads)
trevrpc_cpp_warnings(trevrpc_cpp_bench_peer)
trevrpc_cpp_sanitizers(trevrpc_cpp_bench_peer)

install(TARGETS trevrpc_cpp_bench_peer RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
