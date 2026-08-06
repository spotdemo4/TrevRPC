set(trevrpc_cpp_test_generated_dir "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(trevrpc_cpp_test_proto_dir "${CMAKE_CURRENT_SOURCE_DIR}/proto")

function(trevrpc_cpp_test_assertions target)
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(${target} PRIVATE -UNDEBUG)
  endif()
endfunction()

function(trevrpc_cpp_lsan_suppressions test_name)
  if(TREVRPC_CPP_ENABLE_SANITIZERS)
    set_tests_properties(${test_name} PROPERTIES ENVIRONMENT
      "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/lsan.supp"
    )
  endif()
endfunction()
set(trevrpc_cpp_test_generated_files
  "${trevrpc_cpp_test_generated_dir}/common.pb.cc"
  "${trevrpc_cpp_test_generated_dir}/common.pb.h"
  "${trevrpc_cpp_test_generated_dir}/generator.pb.cc"
  "${trevrpc_cpp_test_generated_dir}/generator.pb.h"
  "${trevrpc_cpp_test_generated_dir}/generator.trevrpc.cpp"
  "${trevrpc_cpp_test_generated_dir}/generator.trevrpc.hpp")

add_custom_command(
  OUTPUT ${trevrpc_cpp_test_generated_files}
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${trevrpc_cpp_test_generated_dir}"
  COMMAND protobuf::protoc
    -I "${trevrpc_cpp_test_proto_dir}"
    --cpp_out=${trevrpc_cpp_test_generated_dir}
    "${trevrpc_cpp_test_proto_dir}/common.proto"
    "${trevrpc_cpp_test_proto_dir}/generator.proto"
  COMMAND protobuf::protoc
    -I "${trevrpc_cpp_test_proto_dir}"
    --plugin=protoc-gen-trevrpc-cpp=$<TARGET_FILE:protoc-gen-trevrpc-cpp>
    --trevrpc-cpp_out=${trevrpc_cpp_test_generated_dir}
    "${trevrpc_cpp_test_proto_dir}/generator.proto"
  DEPENDS
    "${trevrpc_cpp_test_proto_dir}/common.proto"
    "${trevrpc_cpp_test_proto_dir}/generator.proto"
    protoc-gen-trevrpc-cpp
  VERBATIM)

add_library(trevrpc_cpp_test_generated STATIC
  "${trevrpc_cpp_test_generated_dir}/common.pb.cc"
  "${trevrpc_cpp_test_generated_dir}/generator.pb.cc"
  "${trevrpc_cpp_test_generated_dir}/generator.trevrpc.cpp")
target_include_directories(trevrpc_cpp_test_generated PUBLIC "${trevrpc_cpp_test_generated_dir}")
target_link_libraries(trevrpc_cpp_test_generated PUBLIC trevrpc_cpp protobuf::libprotobuf-lite)
trevrpc_cpp_warnings(trevrpc_cpp_test_generated)
trevrpc_cpp_sanitizers(trevrpc_cpp_test_generated)

add_custom_command(
  OUTPUT
    "${trevrpc_cpp_test_generated_dir}/full.pb.cc"
    "${trevrpc_cpp_test_generated_dir}/full.pb.h"
  COMMAND "${CMAKE_COMMAND}" -E make_directory "${trevrpc_cpp_test_generated_dir}"
  COMMAND protobuf::protoc
    -I "${trevrpc_cpp_test_proto_dir}"
    --cpp_out=${trevrpc_cpp_test_generated_dir}
    "${trevrpc_cpp_test_proto_dir}/full.proto"
  DEPENDS "${trevrpc_cpp_test_proto_dir}/full.proto"
  VERBATIM)

add_executable(trevrpc_cpp_codec_test
  codec_test.cpp
  "${trevrpc_cpp_test_generated_dir}/full.pb.cc")
target_include_directories(trevrpc_cpp_codec_test PRIVATE "${trevrpc_cpp_test_generated_dir}")
target_link_libraries(trevrpc_cpp_codec_test PRIVATE trevrpc_cpp_test_generated protobuf::libprotobuf)
trevrpc_cpp_warnings(trevrpc_cpp_codec_test)
trevrpc_cpp_sanitizers(trevrpc_cpp_codec_test)
trevrpc_cpp_test_assertions(trevrpc_cpp_codec_test)
add_test(NAME trevrpc_cpp_codec COMMAND trevrpc_cpp_codec_test)

add_executable(trevrpc_cpp_generator_options_test
  generator_options_test.cpp
  ../tools/protoc-gen-trevrpc-cpp/generator.cpp)
target_include_directories(trevrpc_cpp_generator_options_test PRIVATE
  ../tools/protoc-gen-trevrpc-cpp)
target_link_libraries(trevrpc_cpp_generator_options_test PRIVATE
  protobuf::libprotoc protobuf::libprotobuf)
trevrpc_cpp_warnings(trevrpc_cpp_generator_options_test)
trevrpc_cpp_sanitizers(trevrpc_cpp_generator_options_test)
trevrpc_cpp_test_assertions(trevrpc_cpp_generator_options_test)
add_test(NAME trevrpc_cpp_generator_options COMMAND trevrpc_cpp_generator_options_test)

add_test(NAME trevrpc_cpp_generator_collisions
  COMMAND "${CMAKE_COMMAND}"
    -DPROTOC=$<TARGET_FILE:protobuf::protoc>
    -DPLUGIN=$<TARGET_FILE:protoc-gen-trevrpc-cpp>
    -DPROTO_DIR=${trevrpc_cpp_test_proto_dir}
    -DOUTPUT_DIR=${trevrpc_cpp_test_generated_dir}/collisions
    -P "${CMAKE_CURRENT_SOURCE_DIR}/generator-collision-test.cmake")

add_test(NAME trevrpc_cpp_generator_header_drift
  COMMAND "${CMAKE_COMMAND}" -E compare_files
    "${trevrpc_cpp_test_generated_dir}/generator.trevrpc.hpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/golden/generator.trevrpc.hpp.golden")
add_test(NAME trevrpc_cpp_generator_source_drift
  COMMAND "${CMAKE_COMMAND}" -E compare_files
    "${trevrpc_cpp_test_generated_dir}/generator.trevrpc.cpp"
    "${CMAKE_CURRENT_SOURCE_DIR}/golden/generator.trevrpc.cpp.golden")

find_program(OPENSSL_EXECUTABLE openssl REQUIRED)
set(trevrpc_cpp_test_cert "${CMAKE_CURRENT_BINARY_DIR}/test-cert.pem")
set(trevrpc_cpp_test_key "${CMAKE_CURRENT_BINARY_DIR}/test-key.pem")
add_custom_command(
  OUTPUT "${trevrpc_cpp_test_cert}" "${trevrpc_cpp_test_key}"
  COMMAND "${OPENSSL_EXECUTABLE}" req -x509 -newkey ec
    -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 1
    -subj /CN=localhost
    -addext basicConstraints=critical,CA:FALSE
    -addext keyUsage=critical,digitalSignature
    -addext extendedKeyUsage=serverAuth
    -addext subjectAltName=DNS:localhost,IP:127.0.0.1
    -keyout "${trevrpc_cpp_test_key}"
    -out "${trevrpc_cpp_test_cert}"
  VERBATIM)
add_custom_target(trevrpc_cpp_test_certificates
  DEPENDS "${trevrpc_cpp_test_cert}" "${trevrpc_cpp_test_key}")

add_executable(trevrpc_cpp_generated_service_test generated_service_test.cpp)
add_dependencies(trevrpc_cpp_generated_service_test trevrpc_cpp_test_certificates)
target_compile_definitions(trevrpc_cpp_generated_service_test PRIVATE
  TREVRPC_CPP_TEST_CERT="${trevrpc_cpp_test_cert}"
  TREVRPC_CPP_TEST_KEY="${trevrpc_cpp_test_key}")
target_link_libraries(trevrpc_cpp_generated_service_test PRIVATE trevrpc_cpp_test_generated)
trevrpc_cpp_warnings(trevrpc_cpp_generated_service_test)
trevrpc_cpp_sanitizers(trevrpc_cpp_generated_service_test)
trevrpc_cpp_test_assertions(trevrpc_cpp_generated_service_test)
add_test(NAME trevrpc_cpp_generated_service COMMAND trevrpc_cpp_generated_service_test)
set_tests_properties(trevrpc_cpp_generated_service PROPERTIES TIMEOUT 30)
trevrpc_cpp_lsan_suppressions(trevrpc_cpp_generated_service)


add_executable(trevrpc_cpp_async_core_test async_core_test.cpp)
target_link_libraries(trevrpc_cpp_async_core_test PRIVATE trevrpc_cpp)
trevrpc_cpp_warnings(trevrpc_cpp_async_core_test)
trevrpc_cpp_sanitizers(trevrpc_cpp_async_core_test)
trevrpc_cpp_test_assertions(trevrpc_cpp_async_core_test)
add_test(NAME trevrpc_cpp_async_core COMMAND trevrpc_cpp_async_core_test)
set_tests_properties(trevrpc_cpp_async_core PROPERTIES TIMEOUT 15)

add_executable(trevrpc_cpp_async_stream_test async_stream_test.cpp)
target_include_directories(trevrpc_cpp_async_stream_test PRIVATE ../src)
target_link_libraries(trevrpc_cpp_async_stream_test PRIVATE trevrpc_cpp)
trevrpc_cpp_warnings(trevrpc_cpp_async_stream_test)
trevrpc_cpp_sanitizers(trevrpc_cpp_async_stream_test)
trevrpc_cpp_test_assertions(trevrpc_cpp_async_stream_test)
add_test(NAME trevrpc_cpp_async_stream COMMAND trevrpc_cpp_async_stream_test)
set_tests_properties(trevrpc_cpp_async_stream PROPERTIES TIMEOUT 15)

add_executable(trevrpc_cpp_async_server_core_test async_server_core_test.cpp)
target_include_directories(trevrpc_cpp_async_server_core_test PRIVATE ../src)
target_link_libraries(trevrpc_cpp_async_server_core_test PRIVATE trevrpc_cpp)
trevrpc_cpp_warnings(trevrpc_cpp_async_server_core_test)
trevrpc_cpp_sanitizers(trevrpc_cpp_async_server_core_test)
trevrpc_cpp_test_assertions(trevrpc_cpp_async_server_core_test)
add_test(NAME trevrpc_cpp_async_server_core COMMAND trevrpc_cpp_async_server_core_test)
set_tests_properties(trevrpc_cpp_async_server_core PROPERTIES TIMEOUT 15)

add_executable(trevrpc_cpp_callbacks_test callbacks_test.cpp)
add_dependencies(trevrpc_cpp_callbacks_test trevrpc_cpp_test_certificates)
target_include_directories(trevrpc_cpp_callbacks_test PRIVATE ../src)
target_compile_definitions(trevrpc_cpp_callbacks_test PRIVATE
  TREVRPC_CPP_TEST_CERT="${trevrpc_cpp_test_cert}"
  TREVRPC_CPP_TEST_KEY="${trevrpc_cpp_test_key}")
target_link_libraries(trevrpc_cpp_callbacks_test PRIVATE trevrpc_cpp)
trevrpc_cpp_warnings(trevrpc_cpp_callbacks_test)
trevrpc_cpp_sanitizers(trevrpc_cpp_callbacks_test)
trevrpc_cpp_test_assertions(trevrpc_cpp_callbacks_test)
add_test(NAME trevrpc_cpp_callbacks COMMAND trevrpc_cpp_callbacks_test)
set_tests_properties(trevrpc_cpp_callbacks PROPERTIES TIMEOUT 20)
trevrpc_cpp_lsan_suppressions(trevrpc_cpp_callbacks)

add_executable(trevrpc_cpp_server_lifecycle_test server_lifecycle_test.cpp)
add_dependencies(trevrpc_cpp_server_lifecycle_test trevrpc_cpp_test_certificates)
target_include_directories(trevrpc_cpp_server_lifecycle_test PRIVATE ../src)
target_compile_definitions(trevrpc_cpp_server_lifecycle_test PRIVATE
  TREVRPC_CPP_TEST_CERT="${trevrpc_cpp_test_cert}"
  TREVRPC_CPP_TEST_KEY="${trevrpc_cpp_test_key}")
target_link_libraries(trevrpc_cpp_server_lifecycle_test PRIVATE trevrpc_cpp)
trevrpc_cpp_warnings(trevrpc_cpp_server_lifecycle_test)
trevrpc_cpp_sanitizers(trevrpc_cpp_server_lifecycle_test)
trevrpc_cpp_test_assertions(trevrpc_cpp_server_lifecycle_test)
add_test(NAME trevrpc_cpp_server_lifecycle COMMAND trevrpc_cpp_server_lifecycle_test)
set_tests_properties(trevrpc_cpp_server_lifecycle PROPERTIES TIMEOUT 15)
trevrpc_cpp_lsan_suppressions(trevrpc_cpp_server_lifecycle)

add_executable(trevrpc_cpp_generated_async_service_test generated_async_service_test.cpp)
add_dependencies(trevrpc_cpp_generated_async_service_test trevrpc_cpp_test_certificates)
target_compile_definitions(trevrpc_cpp_generated_async_service_test PRIVATE
  TREVRPC_CPP_TEST_CERT="${trevrpc_cpp_test_cert}"
  TREVRPC_CPP_TEST_KEY="${trevrpc_cpp_test_key}")
target_link_libraries(trevrpc_cpp_generated_async_service_test PRIVATE trevrpc_cpp_test_generated)
trevrpc_cpp_warnings(trevrpc_cpp_generated_async_service_test)
trevrpc_cpp_sanitizers(trevrpc_cpp_generated_async_service_test)
trevrpc_cpp_test_assertions(trevrpc_cpp_generated_async_service_test)
add_test(NAME trevrpc_cpp_generated_async_service COMMAND trevrpc_cpp_generated_async_service_test)
set_tests_properties(trevrpc_cpp_generated_async_service PROPERTIES TIMEOUT 30)
trevrpc_cpp_lsan_suppressions(trevrpc_cpp_generated_async_service)
