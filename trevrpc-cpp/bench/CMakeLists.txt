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

add_library(trevrpc_cpp_benchmark_adapters STATIC benchmark_peer.cpp)
target_include_directories(trevrpc_cpp_benchmark_adapters PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_link_libraries(trevrpc_cpp_benchmark_adapters
  PUBLIC trevrpc_cpp_benchmark_generated)
trevrpc_cpp_warnings(trevrpc_cpp_benchmark_adapters)
trevrpc_cpp_sanitizers(trevrpc_cpp_benchmark_adapters)

add_executable(trevrpc_cpp_bench_peer trevrpc_bench_peer.cpp)
set_target_properties(trevrpc_cpp_bench_peer PROPERTIES OUTPUT_NAME trevrpc-bench-peer-cpp)
target_link_libraries(trevrpc_cpp_bench_peer PRIVATE
  trevrpc_cpp_benchmark_adapters
  Threads::Threads)
trevrpc_cpp_warnings(trevrpc_cpp_bench_peer)
trevrpc_cpp_sanitizers(trevrpc_cpp_bench_peer)

function(trevrpc_cpp_bench_lsan_suppressions test_name)
  if(TREVRPC_CPP_ENABLE_SANITIZERS)
    set_tests_properties(${test_name} PROPERTIES ENVIRONMENT
      "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/../tests/lsan.supp"
    )
  endif()
endfunction()

if(TREVRPC_CPP_BUILD_TESTS)
  add_test(NAME trevrpc_cpp_benchmark_capabilities COMMAND trevrpc_cpp_bench_peer capabilities)
  set_tests_properties(trevrpc_cpp_benchmark_capabilities PROPERTIES
    PASS_REGULAR_EXPRESSION
      "schema_version.:4.*roles.*client.*trevrpc_native_quic.*server.*trevrpc_native_quic.*trevrpc_webtransport.*rpc_kinds.*unary.*client_stream.*server_stream.*bidi.*histogram.*log_linear_v1")

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
  add_test(NAME trevrpc_cpp_benchmark_webtransport_config
    COMMAND "${CMAKE_COMMAND}"
      -DPEER=$<TARGET_FILE:trevrpc_cpp_bench_peer>
      -P "${CMAKE_CURRENT_SOURCE_DIR}/webtransport-config-test.cmake")

  find_program(trevrpc_cpp_benchmark_openssl openssl REQUIRED)
  set(trevrpc_cpp_benchmark_test_cert "${CMAKE_CURRENT_BINARY_DIR}/test-cert.pem")
  set(trevrpc_cpp_benchmark_test_key "${CMAKE_CURRENT_BINARY_DIR}/test-key.pem")
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

  add_executable(trevrpc_cpp_webtransport_probe webtransport_probe.cpp)
  target_link_libraries(trevrpc_cpp_webtransport_probe PRIVATE ${trevrpc_cpp_c_target})
  trevrpc_cpp_warnings(trevrpc_cpp_webtransport_probe)
  trevrpc_cpp_sanitizers(trevrpc_cpp_webtransport_probe)

  add_test(NAME trevrpc_cpp_benchmark_webtransport_smoke
    COMMAND sh
      "${CMAKE_CURRENT_SOURCE_DIR}/webtransport-smoke-test.sh"
      "$<TARGET_FILE:trevrpc_cpp_bench_peer>"
      "$<TARGET_FILE:trevrpc_cpp_webtransport_probe>"
      "${trevrpc_cpp_benchmark_test_cert}"
      "${trevrpc_cpp_benchmark_test_key}")
  add_dependencies(trevrpc_cpp_bench_peer trevrpc_cpp_benchmark_test_certificates)
  set_tests_properties(trevrpc_cpp_benchmark_webtransport_smoke PROPERTIES TIMEOUT 30)
  trevrpc_cpp_bench_lsan_suppressions(trevrpc_cpp_benchmark_webtransport_smoke)

endif()

install(TARGETS trevrpc_cpp_bench_peer
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
  COMPONENT benchmark-peer)
