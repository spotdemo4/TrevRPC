trevrpc_cpp_generate(
  TARGET trevrpc_cpp_greeter_generated
  PROTO "${CMAKE_CURRENT_SOURCE_DIR}/greeter/greeter.proto"
  IMPORT_DIR "${CMAKE_CURRENT_SOURCE_DIR}/greeter"
  OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")

add_executable(trevrpc_cpp_greeter_server greeter/server.cpp)
target_link_libraries(trevrpc_cpp_greeter_server PRIVATE trevrpc_cpp_greeter_generated)
trevrpc_cpp_warnings(trevrpc_cpp_greeter_server)
trevrpc_cpp_sanitizers(trevrpc_cpp_greeter_server)

add_executable(trevrpc_cpp_greeter_client greeter/client.cpp)
target_link_libraries(trevrpc_cpp_greeter_client PRIVATE trevrpc_cpp_greeter_generated)
trevrpc_cpp_warnings(trevrpc_cpp_greeter_client)
trevrpc_cpp_sanitizers(trevrpc_cpp_greeter_client)
