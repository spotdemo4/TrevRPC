cmake_minimum_required(VERSION 3.20)

project(trevrpc-c VERSION 0.1.0 LANGUAGES C)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

option(TREVRPC_BUILD_TESTS "Build trevrpc-c tests" ON)
option(TREVRPC_BUILD_BENCHMARKS "Build trevrpc-c benchmark executables" OFF)
option(TREVRPC_BUILD_EXAMPLES "Build trevrpc-c example executables" "${TREVRPC_BUILD_TESTS}")
option(TREVRPC_BUILD_MSQUIC "Build trevrpc-c MsQuic transport" ON)
option(TREVRPC_BUILD_WEBTRANSPORT "Build trevrpc-c WebTransport transport" ON)
option(TREVRPC_BUILD_RUNTIME "Build trevrpc-c high-level runtime" ON)
option(TREVRPC_INSTALL_INTERNAL_HEADERS "Install trevrpc-c internal headers for tests and development" OFF)
option(TREVRPC_ENABLE_SANITIZERS "Build trevrpc-c with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)

find_package(Threads REQUIRED)

add_library(trevrpc_core STATIC
    src/trevrpc_values.c
    src/trevrpc_wire.c
)
target_include_directories(trevrpc_core
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    PRIVATE src
)

function(trevrpc_c_warnings target)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(${target} PRIVATE -Wall -Wextra -Werror)
    endif()
endfunction()

function(trevrpc_c_sanitizers target)
    if(TREVRPC_ENABLE_SANITIZERS AND CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(${target} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
        target_link_options(${target} PRIVATE -fsanitize=address,undefined)
    endif()
endfunction()

trevrpc_c_warnings(trevrpc_core)
trevrpc_c_sanitizers(trevrpc_core)

set(trevrpc_install_targets trevrpc_core)
set(trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_core.pc")

configure_file(cmake/trevrpc_core.pc.in "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_core.pc" @ONLY)

if(TREVRPC_BUILD_MSQUIC)
    find_path(MSQUIC_INCLUDE_DIR NAMES msquic.h inc/msquic.h REQUIRED)
    find_library(MSQUIC_LIBRARY NAMES msquic REQUIRED)

    add_library(trevrpc_msquic STATIC
        src/trevrpc_msquic.c
    )
    target_include_directories(trevrpc_msquic
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE ${MSQUIC_INCLUDE_DIR}
    )
    target_link_libraries(trevrpc_msquic PUBLIC Threads::Threads PRIVATE ${MSQUIC_LIBRARY})
    trevrpc_c_warnings(trevrpc_msquic)
    trevrpc_c_sanitizers(trevrpc_msquic)
    list(APPEND trevrpc_install_targets trevrpc_msquic)
    list(APPEND trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_msquic.pc")
    configure_file(cmake/trevrpc_msquic.pc.in "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_msquic.pc" @ONLY)
endif()

if(TREVRPC_BUILD_WEBTRANSPORT)
    add_library(trevrpc_webtransport STATIC
        src/trevrpc_webtransport.c
    )
    target_include_directories(trevrpc_webtransport
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )
    target_link_libraries(trevrpc_webtransport PUBLIC Threads::Threads)
    trevrpc_c_warnings(trevrpc_webtransport)
    trevrpc_c_sanitizers(trevrpc_webtransport)
    list(APPEND trevrpc_install_targets trevrpc_webtransport)
    list(APPEND trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_webtransport.pc")
    configure_file(cmake/trevrpc_webtransport.pc.in "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_webtransport.pc" @ONLY)
endif()

if(TREVRPC_BUILD_RUNTIME)
    if(NOT TREVRPC_BUILD_MSQUIC)
        message(FATAL_ERROR "TREVRPC_BUILD_RUNTIME requires TREVRPC_BUILD_MSQUIC")
    endif()
    if(NOT TREVRPC_BUILD_WEBTRANSPORT)
        message(FATAL_ERROR "TREVRPC_BUILD_RUNTIME requires TREVRPC_BUILD_WEBTRANSPORT")
    endif()

    add_library(trevrpc STATIC
        src/trevrpc.c
    )
    target_include_directories(trevrpc
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE src
    )
    target_link_libraries(trevrpc PUBLIC trevrpc_core trevrpc_msquic trevrpc_webtransport Threads::Threads)
    trevrpc_c_warnings(trevrpc)
    trevrpc_c_sanitizers(trevrpc)
    list(APPEND trevrpc_install_targets trevrpc)
    list(APPEND trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc.pc")
    configure_file(cmake/trevrpc.pc.in "${CMAKE_CURRENT_BINARY_DIR}/trevrpc.pc" @ONLY)
endif()

if(TREVRPC_BUILD_RUNTIME AND (TREVRPC_BUILD_TESTS OR TREVRPC_BUILD_BENCHMARKS OR TREVRPC_BUILD_EXAMPLES))
    find_program(PROTOC_EXECUTABLE protoc REQUIRED)
    find_program(PROTOC_C_EXECUTABLE protoc-c REQUIRED)
    find_program(TREVRPC_C_GENERATOR_EXECUTABLE protoc-gen-trevrpc-c)

    if(NOT TREVRPC_C_GENERATOR_EXECUTABLE)
        set(trevrpc_generator_build_dir "${CMAKE_CURRENT_BINARY_DIR}/protoc-gen-trevrpc-c-build")
        find_program(GO_EXECUTABLE go REQUIRED)
        set(TREVRPC_C_GENERATOR_EXECUTABLE "${trevrpc_generator_build_dir}/protoc-gen-trevrpc-c")
        add_custom_command(
            OUTPUT "${TREVRPC_C_GENERATOR_EXECUTABLE}"
            COMMAND "${CMAKE_COMMAND}" -E make_directory "${trevrpc_generator_build_dir}"
            COMMAND "${CMAKE_COMMAND}" -E chdir "${CMAKE_CURRENT_SOURCE_DIR}/../trevrpc-go" "${GO_EXECUTABLE}" build -o "${TREVRPC_C_GENERATOR_EXECUTABLE}" ./cmd/protoc-gen-trevrpc-c
            DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/../trevrpc-go/cmd/protoc-gen-trevrpc-c/main.go"
            VERBATIM
        )
    endif()
    function(trevrpc_generate_c_service_bindings output_dir proto_file proto_include_dir output_base out_pb_c out_trevrpc_c)
        set(pb_c "${output_dir}/${output_base}.pb-c.c")
        set(trevrpc_c "${output_dir}/${output_base}.trevrpc.c")
        add_custom_command(
            OUTPUT
                "${pb_c}"
                "${output_dir}/${output_base}.pb-c.h"
                "${trevrpc_c}"
                "${output_dir}/${output_base}.trevrpc.h"
            COMMAND "${CMAKE_COMMAND}" -E make_directory "${output_dir}"
            COMMAND "${PROTOC_C_EXECUTABLE}" --c_out=${output_dir} -I "${proto_include_dir}" "${proto_file}"
            COMMAND "${PROTOC_EXECUTABLE}" --plugin=protoc-gen-trevrpc-c=${TREVRPC_C_GENERATOR_EXECUTABLE} --trevrpc-c_out=${output_dir} -I "${proto_include_dir}" "${proto_file}"
            DEPENDS "${proto_file}" "${TREVRPC_C_GENERATOR_EXECUTABLE}"
            VERBATIM
        )
        set(${out_pb_c} "${pb_c}" PARENT_SCOPE)
        set(${out_trevrpc_c} "${trevrpc_c}" PARENT_SCOPE)
    endfunction()

    if(TREVRPC_BUILD_TESTS OR TREVRPC_BUILD_BENCHMARKS)
        find_program(OPENSSL_EXECUTABLE openssl REQUIRED)
        set(trevrpc_generated_test_dir "${CMAKE_CURRENT_BINARY_DIR}/generated-service-test")
        set(trevrpc_generated_proto "${CMAKE_CURRENT_SOURCE_DIR}/tests/proto/greeter.proto")
        trevrpc_generate_c_service_bindings(
            "${trevrpc_generated_test_dir}"
            "${trevrpc_generated_proto}"
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/proto"
            greeter
            trevrpc_generated_pb_c
            trevrpc_generated_trevrpc_c
        )

        set(trevrpc_msquic_test_cert "${CMAKE_CURRENT_BINARY_DIR}/msquic-test-cert.pem")
        set(trevrpc_msquic_test_key "${CMAKE_CURRENT_BINARY_DIR}/msquic-test-key.pem")
        add_custom_command(
            OUTPUT "${trevrpc_msquic_test_cert}" "${trevrpc_msquic_test_key}"
            COMMAND ${OPENSSL_EXECUTABLE} req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 1 -subj /CN=localhost -keyout "${trevrpc_msquic_test_key}" -out "${trevrpc_msquic_test_cert}"
            VERBATIM
        )
        add_custom_target(trevrpc_msquic_test_certificates DEPENDS "${trevrpc_msquic_test_cert}" "${trevrpc_msquic_test_key}")
    endif()
endif()

if(TREVRPC_BUILD_RUNTIME AND TREVRPC_BUILD_EXAMPLES)
    set(trevrpc_generated_greeter_example_dir "${CMAKE_CURRENT_BINARY_DIR}/generated-greeter-example")
    trevrpc_generate_c_service_bindings(
        "${trevrpc_generated_greeter_example_dir}"
        "${CMAKE_CURRENT_SOURCE_DIR}/examples/greeter/greeter.proto"
        "${CMAKE_CURRENT_SOURCE_DIR}/examples/greeter"
        greeter
        trevrpc_generated_greeter_example_pb_c
        trevrpc_generated_greeter_example_trevrpc_c
    )

    add_library(trevrpc_greeter_example_generated STATIC
        "${trevrpc_generated_greeter_example_pb_c}"
        "${trevrpc_generated_greeter_example_trevrpc_c}"
    )
    target_include_directories(trevrpc_greeter_example_generated PUBLIC "${trevrpc_generated_greeter_example_dir}")
    target_link_libraries(trevrpc_greeter_example_generated PUBLIC trevrpc protobuf-c)
    trevrpc_c_warnings(trevrpc_greeter_example_generated)
    trevrpc_c_sanitizers(trevrpc_greeter_example_generated)

    add_executable(trevrpc_greeter_server examples/greeter/server.c)
    target_link_libraries(trevrpc_greeter_server PRIVATE trevrpc_greeter_example_generated)
    trevrpc_c_warnings(trevrpc_greeter_server)
    trevrpc_c_sanitizers(trevrpc_greeter_server)

    add_executable(trevrpc_greeter_client examples/greeter/client.c)
    target_link_libraries(trevrpc_greeter_client PRIVATE trevrpc_greeter_example_generated)
    trevrpc_c_warnings(trevrpc_greeter_client)
    trevrpc_c_sanitizers(trevrpc_greeter_client)
endif()

if(TREVRPC_BUILD_BENCHMARKS)
    add_executable(trevrpc_wire_bench bench/wire_bench.c)
    target_include_directories(trevrpc_wire_bench PRIVATE src)
    target_link_libraries(trevrpc_wire_bench PRIVATE trevrpc_core)
    trevrpc_c_warnings(trevrpc_wire_bench)
    trevrpc_c_sanitizers(trevrpc_wire_bench)

    if(TREVRPC_BUILD_RUNTIME)
        add_executable(trevrpc_runtime_bench bench/runtime_bench.c)
        target_include_directories(trevrpc_runtime_bench PRIVATE src)
        target_compile_definitions(trevrpc PRIVATE TREVRPC_TESTING)
        target_link_libraries(trevrpc_runtime_bench PRIVATE trevrpc)
        trevrpc_c_warnings(trevrpc_runtime_bench)
        trevrpc_c_sanitizers(trevrpc_runtime_bench)

        add_executable(trevrpc_rpc_comparison_bench
            bench/rpc_comparison_bench.c
            "${trevrpc_generated_pb_c}"
            "${trevrpc_generated_trevrpc_c}"
        )
        add_dependencies(trevrpc_rpc_comparison_bench trevrpc_msquic_test_certificates)
        target_compile_definitions(trevrpc_rpc_comparison_bench PRIVATE
            TREVRPC_MSQUIC_TEST_CERT="${trevrpc_msquic_test_cert}"
            TREVRPC_MSQUIC_TEST_KEY="${trevrpc_msquic_test_key}"
        )
        target_include_directories(trevrpc_rpc_comparison_bench PRIVATE src "${trevrpc_generated_test_dir}")
        target_link_libraries(trevrpc_rpc_comparison_bench PRIVATE trevrpc protobuf-c)
        trevrpc_c_warnings(trevrpc_rpc_comparison_bench)
        trevrpc_c_sanitizers(trevrpc_rpc_comparison_bench)
    endif()
endif()

install(TARGETS ${trevrpc_install_targets}
    EXPORT trevrpcTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(EXPORT trevrpcTargets
    NAMESPACE trevrpc::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/trevrpc
)
configure_package_config_file(
    cmake/trevrpcConfig.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/trevrpcConfig.cmake"
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/trevrpc
)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/trevrpcConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/trevrpcConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/trevrpcConfigVersion.cmake"
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/trevrpc
)
install(FILES ${trevrpc_pkg_config_files}
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig
)
install(FILES
    include/trevrpc.h
    include/trevrpc_msquic.h
    include/trevrpc_webtransport.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
if(TREVRPC_INSTALL_INTERNAL_HEADERS)
    install(FILES
        src/trevrpc_wire_internal.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/trevrpc/internal
    )
endif()

if(TREVRPC_BUILD_TESTS)
    enable_testing()

    add_executable(trevrpc_wire_test tests/wire_test.c)
    target_include_directories(trevrpc_wire_test PRIVATE src)
    target_compile_definitions(trevrpc_wire_test PRIVATE TREVRPC_WIRE_GOLDEN_VECTORS="${CMAKE_CURRENT_SOURCE_DIR}/../testdata/wire-golden-vectors.txt")
    target_link_libraries(trevrpc_wire_test PRIVATE trevrpc_core)
    trevrpc_c_warnings(trevrpc_wire_test)
    trevrpc_c_sanitizers(trevrpc_wire_test)

    add_test(NAME trevrpc_wire COMMAND trevrpc_wire_test)

    if(TREVRPC_BUILD_RUNTIME)
        add_executable(trevrpc_runtime_test tests/runtime_test.c)
        target_include_directories(trevrpc_runtime_test PRIVATE src)
        target_compile_definitions(trevrpc PRIVATE TREVRPC_TESTING)
        target_link_libraries(trevrpc_runtime_test PRIVATE trevrpc)
        trevrpc_c_warnings(trevrpc_runtime_test)
        trevrpc_c_sanitizers(trevrpc_runtime_test)

        add_test(NAME trevrpc_runtime COMMAND trevrpc_runtime_test)

        add_executable(trevrpc_msquic_transport_test tests/msquic_transport_test.c)
        add_dependencies(trevrpc_msquic_transport_test trevrpc_msquic_test_certificates)
        target_compile_definitions(trevrpc_msquic_transport_test PRIVATE
            TREVRPC_MSQUIC_TEST_CERT="${trevrpc_msquic_test_cert}"
            TREVRPC_MSQUIC_TEST_KEY="${trevrpc_msquic_test_key}"
        )
        target_link_libraries(trevrpc_msquic_transport_test PRIVATE trevrpc_msquic trevrpc_webtransport)
        trevrpc_c_warnings(trevrpc_msquic_transport_test)
        trevrpc_c_sanitizers(trevrpc_msquic_transport_test)

        add_test(NAME trevrpc_msquic_transport COMMAND trevrpc_msquic_transport_test)

        add_executable(trevrpc_generated_service_test
            tests/generated_service_test.c
            "${trevrpc_generated_pb_c}"
            "${trevrpc_generated_trevrpc_c}"
        )
        add_dependencies(trevrpc_generated_service_test trevrpc_msquic_test_certificates)
        target_compile_definitions(trevrpc_generated_service_test PRIVATE
            TREVRPC_MSQUIC_TEST_CERT="${trevrpc_msquic_test_cert}"
            TREVRPC_MSQUIC_TEST_KEY="${trevrpc_msquic_test_key}"
        )
        target_include_directories(trevrpc_generated_service_test PRIVATE src "${trevrpc_generated_test_dir}")
        target_link_libraries(trevrpc_generated_service_test PRIVATE trevrpc protobuf-c)
        trevrpc_c_warnings(trevrpc_generated_service_test)
        trevrpc_c_sanitizers(trevrpc_generated_service_test)

        add_test(NAME trevrpc_generated_service COMMAND trevrpc_generated_service_test)
    endif()
endif()
