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

find_package(gRPC CONFIG REQUIRED)
find_package(Threads REQUIRED)

set(trevrpc_cpp_benchmark_generated_dir "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(trevrpc_cpp_benchmark_grpc_sources
  "${trevrpc_cpp_benchmark_generated_dir}/benchmark.grpc.pb.cc"
  "${trevrpc_cpp_benchmark_generated_dir}/benchmark.grpc.pb.h")
add_custom_command(
  OUTPUT ${trevrpc_cpp_benchmark_grpc_sources}
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${trevrpc_cpp_benchmark_generated_dir}"
  COMMAND protobuf::protoc
    -I "${CMAKE_CURRENT_SOURCE_DIR}/../../bench/proto"
    --grpc_out=${trevrpc_cpp_benchmark_generated_dir}
    --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>
    "${trevrpc_cpp_benchmark_proto}"
  DEPENDS
    "${trevrpc_cpp_benchmark_proto}"
    protobuf::protoc
    gRPC::grpc_cpp_plugin
  VERBATIM)

add_library(trevrpc_cpp_benchmark_grpc_generated STATIC
  "${trevrpc_cpp_benchmark_generated_dir}/benchmark.grpc.pb.cc")
target_include_directories(trevrpc_cpp_benchmark_grpc_generated
  PUBLIC "${trevrpc_cpp_benchmark_generated_dir}")
target_link_libraries(trevrpc_cpp_benchmark_grpc_generated
  PUBLIC
    trevrpc_cpp_benchmark_generated
    gRPC::grpc++)

add_library(trevrpc_cpp_benchmark_adapters STATIC
  benchmark_peer.cpp
  grpc_adapter.cpp)
target_include_directories(trevrpc_cpp_benchmark_adapters PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(trevrpc_cpp_benchmark_adapters
  PUBLIC trevrpc_cpp_benchmark_grpc_generated)
trevrpc_cpp_warnings(trevrpc_cpp_benchmark_adapters)
trevrpc_cpp_sanitizers(trevrpc_cpp_benchmark_adapters)

add_executable(trevrpc_cpp_bench_peer trevrpc_bench_peer.cpp)
set_target_properties(trevrpc_cpp_bench_peer PROPERTIES OUTPUT_NAME trevrpc-bench-peer-cpp)
target_link_libraries(trevrpc_cpp_bench_peer PRIVATE
  trevrpc_cpp_benchmark_adapters
  Threads::Threads)
trevrpc_cpp_warnings(trevrpc_cpp_bench_peer)
trevrpc_cpp_sanitizers(trevrpc_cpp_bench_peer)

if(TREVRPC_CPP_BUILD_TESTS)
  add_test(NAME trevrpc_cpp_benchmark_capabilities COMMAND trevrpc_cpp_bench_peer capabilities)
  set_tests_properties(trevrpc_cpp_benchmark_capabilities PROPERTIES
    PASS_REGULAR_EXPRESSION
      "schema_version.:2.*stacks.*trevrpc_native_quic.*grpc_http2")

  add_test(NAME trevrpc_cpp_benchmark_server_requires_stack
    COMMAND "${CMAKE_COMMAND}"
      -DPEER=$<TARGET_FILE:trevrpc_cpp_bench_peer>
      -DROLE=server
      -P "${CMAKE_CURRENT_SOURCE_DIR}/requires-stack-test.cmake")
  add_test(NAME trevrpc_cpp_benchmark_client_requires_stack
    COMMAND "${CMAKE_COMMAND}"
      -DPEER=$<TARGET_FILE:trevrpc_cpp_bench_peer>
      -DROLE=client
      -P "${CMAKE_CURRENT_SOURCE_DIR}/requires-stack-test.cmake")

  find_program(trevrpc_cpp_benchmark_openssl openssl REQUIRED)
  set(trevrpc_cpp_benchmark_test_cert "${CMAKE_CURRENT_BINARY_DIR}/grpc-test-cert.pem")
  set(trevrpc_cpp_benchmark_test_key "${CMAKE_CURRENT_BINARY_DIR}/grpc-test-key.pem")
  add_custom_command(
    OUTPUT "${trevrpc_cpp_benchmark_test_cert}" "${trevrpc_cpp_benchmark_test_key}"
    COMMAND "${trevrpc_cpp_benchmark_openssl}" req -x509 -newkey ec
      -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 1
      -subj /CN=localhost
      -addext basicConstraints=critical,CA:FALSE
      -addext keyUsage=critical,digitalSignature
      -addext extendedKeyUsage=serverAuth
      -addext subjectAltName=DNS:localhost,IP:127.0.0.1
      -keyout "${trevrpc_cpp_benchmark_test_key}"
      -out "${trevrpc_cpp_benchmark_test_cert}"
    VERBATIM)
  add_custom_target(trevrpc_cpp_benchmark_test_certificates
    DEPENDS "${trevrpc_cpp_benchmark_test_cert}" "${trevrpc_cpp_benchmark_test_key}")

  add_executable(trevrpc_cpp_grpc_adapter_test grpc_adapter_test.cpp)
  add_dependencies(trevrpc_cpp_grpc_adapter_test trevrpc_cpp_benchmark_test_certificates)
  target_compile_definitions(trevrpc_cpp_grpc_adapter_test PRIVATE
    TREVRPC_CPP_BENCH_TEST_CERT="${trevrpc_cpp_benchmark_test_cert}"
    TREVRPC_CPP_BENCH_TEST_KEY="${trevrpc_cpp_benchmark_test_key}")
  target_link_libraries(trevrpc_cpp_grpc_adapter_test PRIVATE
    trevrpc_cpp_benchmark_adapters)
  trevrpc_cpp_warnings(trevrpc_cpp_grpc_adapter_test)
  trevrpc_cpp_sanitizers(trevrpc_cpp_grpc_adapter_test)
  add_test(NAME trevrpc_cpp_grpc_adapter COMMAND trevrpc_cpp_grpc_adapter_test)
  set_tests_properties(trevrpc_cpp_grpc_adapter PROPERTIES TIMEOUT 30)
endif()

install(TARGETS trevrpc_cpp_bench_peer
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT benchmark-peer)
