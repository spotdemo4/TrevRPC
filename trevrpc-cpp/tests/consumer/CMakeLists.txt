cmake_minimum_required(VERSION 3.20)

project(trevrpc-cpp-consumer LANGUAGES CXX)

find_package(trevrpc-cpp CONFIG REQUIRED)

trevrpc_cpp_generate(
  TARGET trevrpc_cpp_consumer_generated
  PROTO "${CMAKE_CURRENT_SOURCE_DIR}/proto/nested/service.proto"
  IMPORT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/proto"
  OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")

find_program(OPENSSL_EXECUTABLE openssl REQUIRED)
set(consumer_cert "${CMAKE_CURRENT_BINARY_DIR}/consumer-cert.pem")
set(consumer_key "${CMAKE_CURRENT_BINARY_DIR}/consumer-key.pem")
add_custom_command(
  OUTPUT "${consumer_cert}" "${consumer_key}"
  COMMAND "${OPENSSL_EXECUTABLE}" req -x509 -newkey ec
    -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 1
    -subj /CN=localhost
    -addext basicConstraints=critical,CA:FALSE
    -addext keyUsage=critical,digitalSignature
    -addext extendedKeyUsage=serverAuth
    -addext subjectAltName=DNS:localhost,IP:127.0.0.1
    -keyout "${consumer_key}"
    -out "${consumer_cert}"
  VERBATIM)
add_custom_target(trevrpc_cpp_consumer_certificates DEPENDS "${consumer_cert}" "${consumer_key}")

add_executable(trevrpc_cpp_consumer main.cpp)
add_dependencies(trevrpc_cpp_consumer trevrpc_cpp_consumer_certificates)
target_compile_features(trevrpc_cpp_consumer PRIVATE cxx_std_20)
target_compile_definitions(trevrpc_cpp_consumer PRIVATE
  TREVRPC_CPP_CONSUMER_CERT="${consumer_cert}"
  TREVRPC_CPP_CONSUMER_KEY="${consumer_key}")
target_link_libraries(trevrpc_cpp_consumer PRIVATE trevrpc_cpp_consumer_generated)

enable_testing()
add_test(NAME trevrpc_cpp_installed_async_consumer COMMAND trevrpc_cpp_consumer)
set_tests_properties(trevrpc_cpp_installed_async_consumer PROPERTIES TIMEOUT 30)
