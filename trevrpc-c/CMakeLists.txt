cmake_minimum_required(VERSION 3.20)

project(trevrpc-c VERSION 0.2.0 LANGUAGES C)

set(TREVRPC_C_ABI_VERSION 6)

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    set(trevrpc_default_build_codegen ON)
else()
    set(trevrpc_default_build_codegen OFF)
endif()

option(TREVRPC_BUILD_TESTS "Build trevrpc-c tests" ON)
option(TREVRPC_BUILD_BENCHMARKS "Build trevrpc-c benchmark executables" OFF)
option(TREVRPC_BUILD_EXAMPLES "Build trevrpc-c example executables" "${TREVRPC_BUILD_TESTS}")
option(TREVRPC_BUILD_MSQUIC "Build trevrpc-c MsQuic transport" ON)
option(TREVRPC_BUILD_WEBTRANSPORT "Build trevrpc-c WebTransport transport" ON)
option(TREVRPC_BUILD_RUNTIME "Build trevrpc-c high-level runtime" ON)
option(TREVRPC_BUILD_CODEGEN "Build protoc-gen-trevrpc-c" "${trevrpc_default_build_codegen}")
option(TREVRPC_INSTALL_INTERNAL_HEADERS "Install trevrpc-c internal headers for tests and development" OFF)
option(TREVRPC_ENABLE_SANITIZERS "Build trevrpc-c with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(TREVRPC_ENABLE_THREAD_SANITIZER "Build trevrpc-c with ThreadSanitizer" OFF)
if(TREVRPC_ENABLE_SANITIZERS AND TREVRPC_ENABLE_THREAD_SANITIZER)
    message(FATAL_ERROR "TREVRPC_ENABLE_SANITIZERS and TREVRPC_ENABLE_THREAD_SANITIZER are mutually exclusive")
endif()
set(TREVRPC_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/trevrpc" CACHE PATH
    "Install directory for trevrpc CMake package files")
set(TREVRPC_INSTALL_PKGCONFIGDIR "${CMAKE_INSTALL_LIBDIR}/pkgconfig" CACHE PATH
    "Install directory for trevrpc pkg-config files")

find_package(Threads REQUIRED)

add_library(trevrpc_core STATIC
    src/trevrpc_frame.c
    src/trevrpc_ownership.c
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
        target_compile_definitions(${target} PRIVATE TREVRPC_SANITIZER_BUILD)
        target_compile_options(${target} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
        target_link_options(${target} PRIVATE -fsanitize=address,undefined)
    elseif(TREVRPC_ENABLE_THREAD_SANITIZER AND CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_definitions(${target} PRIVATE TREVRPC_SANITIZER_BUILD TREVRPC_THREAD_SANITIZER_BUILD)
        target_compile_options(${target} PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(${target} PRIVATE -fsanitize=thread)
    endif()
endfunction()

function(trevrpc_c_lsan_suppressions test_name)
    if(TREVRPC_ENABLE_SANITIZERS)
        set_tests_properties(${test_name} PROPERTIES ENVIRONMENT
            "LSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/tests/lsan.supp"
        )
    endif()
endfunction()

function(trevrpc_c_tsan_suppressions test_name)
    if(TREVRPC_ENABLE_THREAD_SANITIZER)
        set_property(TEST ${test_name} APPEND PROPERTY ENVIRONMENT
            "TSAN_OPTIONS=suppressions=${CMAKE_CURRENT_SOURCE_DIR}/tests/tsan.supp"
        )
    endif()
endfunction()

function(trevrpc_c_configure_msquic target)
    target_include_directories(${target}
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )
    target_link_libraries(${target} PUBLIC Threads::Threads PRIVATE msquic::msquic trevrpc_core)
    target_compile_definitions(${target} PRIVATE QUIC_API_ENABLE_PREVIEW_FEATURES)
    trevrpc_c_warnings(${target})
    trevrpc_c_sanitizers(${target})
endfunction()

function(trevrpc_c_configure_runtime target)
    target_include_directories(${target}
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE src
    )
    target_link_libraries(${target} PUBLIC ${ARGN} trevrpc_core Threads::Threads)
    trevrpc_c_warnings(${target})
    trevrpc_c_sanitizers(${target})
endfunction()

trevrpc_c_warnings(trevrpc_core)
trevrpc_c_sanitizers(trevrpc_core)

set(trevrpc_install_targets trevrpc_core)
set(trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_core.pc")

if(IS_ABSOLUTE "${CMAKE_INSTALL_LIBDIR}")
    set(trevrpc_pc_libdir "${CMAKE_INSTALL_LIBDIR}")
else()
    set(trevrpc_pc_libdir "\${pcfiledir}/..")
endif()
if(IS_ABSOLUTE "${CMAKE_INSTALL_INCLUDEDIR}")
    set(trevrpc_pc_includedir "${CMAKE_INSTALL_INCLUDEDIR}")
else()
    set(trevrpc_pc_includedir "\${prefix}/${CMAKE_INSTALL_INCLUDEDIR}")
endif()

configure_file(cmake/trevrpc_core.pc.in "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_core.pc" @ONLY)

if(TREVRPC_BUILD_MSQUIC)
    find_package(msquic CONFIG REQUIRED)

    add_library(trevrpc_msquic STATIC
        src/trevrpc_msquic.c
    )
    trevrpc_c_configure_msquic(trevrpc_msquic)
    if(TREVRPC_BUILD_TESTS)
        add_library(trevrpc_msquic_testing STATIC
            src/trevrpc_msquic.c
        )
        target_compile_definitions(trevrpc_msquic_testing PRIVATE TREVRPC_MSQUIC_TESTING)
        trevrpc_c_configure_msquic(trevrpc_msquic_testing)
    endif()
    list(APPEND trevrpc_install_targets trevrpc_msquic)
    list(APPEND trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_msquic.pc")
    configure_file(cmake/trevrpc_msquic.pc.in "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_msquic.pc" @ONLY)
endif()

if(TREVRPC_BUILD_WEBTRANSPORT)
    if(NOT TREVRPC_BUILD_MSQUIC)
        message(FATAL_ERROR "TREVRPC_BUILD_WEBTRANSPORT requires TREVRPC_BUILD_MSQUIC")
    endif()

    add_library(trevrpc_webtransport STATIC
        src/trevrpc_webtransport.c
    )
    target_include_directories(trevrpc_webtransport
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )
    target_link_libraries(trevrpc_webtransport PUBLIC trevrpc_msquic Threads::Threads PRIVATE trevrpc_core)
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
        src/trevrpc_channel.c
        src/trevrpc_lifetime.c
        src/trevrpc_versioned_runtime.c
    )
    trevrpc_c_configure_runtime(trevrpc trevrpc_msquic trevrpc_webtransport)
    list(APPEND trevrpc_install_targets trevrpc)
    list(APPEND trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc.pc")
    configure_file(cmake/trevrpc.pc.in "${CMAKE_CURRENT_BINARY_DIR}/trevrpc.pc" @ONLY)

    if(TREVRPC_BUILD_TESTS)
        add_library(trevrpc_testing STATIC
            src/trevrpc.c
            src/trevrpc_channel.c
            src/trevrpc_lifetime.c
            src/trevrpc_versioned_runtime.c
        )
        target_compile_definitions(trevrpc_testing PRIVATE TREVRPC_TESTING)
        trevrpc_c_configure_runtime(trevrpc_testing trevrpc_msquic trevrpc_webtransport)

        add_library(trevrpc_msquic_testing_runtime STATIC
            src/trevrpc.c
            src/trevrpc_channel.c
            src/trevrpc_lifetime.c
            src/trevrpc_versioned_runtime.c
            src/trevrpc_webtransport.c
        )
        trevrpc_c_configure_runtime(trevrpc_msquic_testing_runtime trevrpc_msquic_testing)
    endif()
endif()

set(trevrpc_needs_generated_bindings OFF)
if(TREVRPC_BUILD_RUNTIME AND (TREVRPC_BUILD_TESTS OR TREVRPC_BUILD_BENCHMARKS OR TREVRPC_BUILD_EXAMPLES))
    set(trevrpc_needs_generated_bindings ON)
endif()

set(TREVRPC_C_GENERATOR_EXECUTABLE "" CACHE FILEPATH "Path to a build-machine protoc-gen-trevrpc-c executable")
set(trevrpc_c_generator_dependency)
if(TREVRPC_BUILD_CODEGEN OR trevrpc_needs_generated_bindings)
    find_program(PROTOC_C_EXECUTABLE protoc-c REQUIRED)

    if(NOT TREVRPC_C_GENERATOR_EXECUTABLE)
        if(CMAKE_CROSSCOMPILING)
            find_program(TREVRPC_C_GENERATOR_EXECUTABLE protoc-gen-trevrpc-c REQUIRED)
        else()
            find_path(PROTOBUF_PROTO_INCLUDE_DIR NAMES google/protobuf/descriptor.proto REQUIRED)
            set(trevrpc_codegen_proto_dir "${CMAKE_CURRENT_BINARY_DIR}/protoc-gen-trevrpc-c-protos")
            set(trevrpc_descriptor_proto_c "${trevrpc_codegen_proto_dir}/google/protobuf/descriptor.pb-c.c")
            set(trevrpc_descriptor_proto_h "${trevrpc_codegen_proto_dir}/google/protobuf/descriptor.pb-c.h")
            set(trevrpc_plugin_proto_c "${trevrpc_codegen_proto_dir}/google/protobuf/compiler/plugin.pb-c.c")
            set(trevrpc_plugin_proto_h "${trevrpc_codegen_proto_dir}/google/protobuf/compiler/plugin.pb-c.h")

            add_custom_command(
                OUTPUT "${trevrpc_descriptor_proto_c}" "${trevrpc_descriptor_proto_h}"
                COMMAND "${CMAKE_COMMAND}" -E make_directory "${trevrpc_codegen_proto_dir}"
                COMMAND "${PROTOC_C_EXECUTABLE}" -I "${PROTOBUF_PROTO_INCLUDE_DIR}" --c_out=${trevrpc_codegen_proto_dir} google/protobuf/descriptor.proto
                DEPENDS "${PROTOBUF_PROTO_INCLUDE_DIR}/google/protobuf/descriptor.proto"
                VERBATIM
            )
            add_custom_command(
                OUTPUT "${trevrpc_plugin_proto_c}" "${trevrpc_plugin_proto_h}"
                COMMAND "${CMAKE_COMMAND}" -E make_directory "${trevrpc_codegen_proto_dir}"
                COMMAND "${PROTOC_C_EXECUTABLE}" -I "${PROTOBUF_PROTO_INCLUDE_DIR}" --c_out=${trevrpc_codegen_proto_dir} google/protobuf/compiler/plugin.proto
                DEPENDS
                    "${PROTOBUF_PROTO_INCLUDE_DIR}/google/protobuf/compiler/plugin.proto"
                    "${trevrpc_descriptor_proto_h}"
                VERBATIM
            )

            add_executable(protoc-gen-trevrpc-c
                tools/protoc-gen-trevrpc-c/main.c
                "${trevrpc_descriptor_proto_c}"
                "${trevrpc_plugin_proto_c}"
            )
            target_include_directories(protoc-gen-trevrpc-c PRIVATE "${trevrpc_codegen_proto_dir}")
            target_link_libraries(protoc-gen-trevrpc-c PRIVATE protobuf-c)
            trevrpc_c_warnings(protoc-gen-trevrpc-c)
            set(TREVRPC_C_GENERATOR_EXECUTABLE "$<TARGET_FILE:protoc-gen-trevrpc-c>")
            set(trevrpc_c_generator_dependency protoc-gen-trevrpc-c)
        endif()
    endif()

    if(NOT TREVRPC_C_GENERATOR_EXECUTABLE)
        message(FATAL_ERROR "TREVRPC_C_GENERATOR_EXECUTABLE was not set")
    endif()
endif()

if(trevrpc_needs_generated_bindings)
    find_program(PROTOC_EXECUTABLE protoc REQUIRED)

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
            DEPENDS "${proto_file}" ${trevrpc_c_generator_dependency}
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

        set(trevrpc_benchmark_proto "${CMAKE_CURRENT_SOURCE_DIR}/bench/proto/benchmark.proto")
        set(trevrpc_canonical_benchmark_proto "${CMAKE_CURRENT_SOURCE_DIR}/../bench/proto/benchmark.proto")
        if(EXISTS "${trevrpc_canonical_benchmark_proto}")
            file(SHA256 "${trevrpc_benchmark_proto}" trevrpc_benchmark_proto_sha256)
            file(SHA256 "${trevrpc_canonical_benchmark_proto}" trevrpc_canonical_benchmark_proto_sha256)
            if(NOT trevrpc_benchmark_proto_sha256 STREQUAL trevrpc_canonical_benchmark_proto_sha256)
                message(FATAL_ERROR "trevrpc-c benchmark.proto differs from the canonical benchmark contract")
            endif()
        endif()
        trevrpc_generate_c_service_bindings(
            "${trevrpc_generated_test_dir}"
            "${trevrpc_benchmark_proto}"
            "${CMAKE_CURRENT_SOURCE_DIR}/bench/proto"
            benchmark
            trevrpc_generated_benchmark_pb_c
            trevrpc_generated_benchmark_trevrpc_c
        )

        add_library(trevrpc_benchmark_generated STATIC
            "${trevrpc_generated_benchmark_pb_c}"
            "${trevrpc_generated_benchmark_trevrpc_c}"
        )
        target_include_directories(trevrpc_benchmark_generated PUBLIC "${trevrpc_generated_test_dir}")
        target_link_libraries(trevrpc_benchmark_generated PUBLIC trevrpc protobuf-c)
        trevrpc_c_warnings(trevrpc_benchmark_generated)
        trevrpc_c_sanitizers(trevrpc_benchmark_generated)

        set(trevrpc_msquic_test_cert "${CMAKE_CURRENT_BINARY_DIR}/msquic-test-cert.pem")
        set(trevrpc_msquic_test_key "${CMAKE_CURRENT_BINARY_DIR}/msquic-test-key.pem")
        add_custom_command(
            OUTPUT "${trevrpc_msquic_test_cert}" "${trevrpc_msquic_test_key}"
            COMMAND ${OPENSSL_EXECUTABLE} req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 1 -subj /CN=localhost -addext "basicConstraints=critical,CA:FALSE" -addext "keyUsage=critical,digitalSignature" -addext "extendedKeyUsage=serverAuth" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" -keyout "${trevrpc_msquic_test_key}" -out "${trevrpc_msquic_test_cert}"
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
    if(TREVRPC_BUILD_RUNTIME)
        find_package(PkgConfig QUIET)
        if(PkgConfig_FOUND)
            pkg_check_modules(GRPC_CORE QUIET IMPORTED_TARGET grpc)
        endif()
        if(TARGET PkgConfig::GRPC_CORE)
            set(trevrpc_grpc_core_target PkgConfig::GRPC_CORE)
        else()
            find_package(gRPC CONFIG REQUIRED)
            set(trevrpc_grpc_core_target gRPC::grpc)
        endif()
        find_package(OpenSSL REQUIRED COMPONENTS Crypto)

        add_executable(trevrpc_bench_peer_c
            bench/trevrpc_bench_grpc.c
            bench/trevrpc_bench_peer.c
        )
        add_dependencies(trevrpc_bench_peer_c trevrpc_msquic_test_certificates)
        set_target_properties(trevrpc_bench_peer_c PROPERTIES OUTPUT_NAME "trevrpc-bench-peer-c")
        target_link_libraries(trevrpc_bench_peer_c PRIVATE
            trevrpc_benchmark_generated
            ${trevrpc_grpc_core_target}
            OpenSSL::Crypto
        )
        trevrpc_c_warnings(trevrpc_bench_peer_c)
        trevrpc_c_sanitizers(trevrpc_bench_peer_c)
    endif()
endif()

install(TARGETS ${trevrpc_install_targets}
    EXPORT trevrpcTargets
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
if(TARGET protoc-gen-trevrpc-c)
    install(TARGETS protoc-gen-trevrpc-c
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
endif()
if(TARGET trevrpc_bench_peer_c)
    install(TARGETS trevrpc_bench_peer_c
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        COMPONENT benchmark-peer
    )
endif()
install(EXPORT trevrpcTargets
    NAMESPACE trevrpc::
    DESTINATION "${TREVRPC_INSTALL_CMAKEDIR}"
)
configure_package_config_file(
    cmake/trevrpcConfig.cmake.in
    "${CMAKE_CURRENT_BINARY_DIR}/trevrpcConfig.cmake"
    INSTALL_DESTINATION "${TREVRPC_INSTALL_CMAKEDIR}"
)
write_basic_package_version_file(
    "${CMAKE_CURRENT_BINARY_DIR}/trevrpcConfigVersion.cmake"
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)
install(FILES
    "${CMAKE_CURRENT_BINARY_DIR}/trevrpcConfig.cmake"
    "${CMAKE_CURRENT_BINARY_DIR}/trevrpcConfigVersion.cmake"
    DESTINATION "${TREVRPC_INSTALL_CMAKEDIR}"
)
install(FILES ${trevrpc_pkg_config_files}
    DESTINATION "${TREVRPC_INSTALL_PKGCONFIGDIR}"
)
install(FILES
    include/trevrpc_binding.h
    include/trevrpc.h
    include/trevrpc_msquic.h
    include/trevrpc_raw.h
    include/trevrpc_webtransport.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
if(TREVRPC_INSTALL_INTERNAL_HEADERS)
    install(FILES
        src/trevrpc_owned_bytes_internal.h
        src/trevrpc_wire_internal.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/trevrpc/internal
    )
endif()

if(TREVRPC_BUILD_TESTS)
    enable_testing()
    enable_language(CXX)

    add_test(NAME trevrpc_abi6_source_surface
        COMMAND "${CMAKE_COMMAND}"
            -DTREVRPC_SOURCE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}/..
            -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/abi6/check_surface.cmake"
    )

    add_executable(trevrpc_frame_test tests/frame_test.c)
    target_include_directories(trevrpc_frame_test PRIVATE src)
    target_link_libraries(trevrpc_frame_test PRIVATE trevrpc_core)
    trevrpc_c_warnings(trevrpc_frame_test)
    trevrpc_c_sanitizers(trevrpc_frame_test)

    add_test(NAME trevrpc_frame COMMAND trevrpc_frame_test)

    add_executable(trevrpc_wire_test tests/wire_test.c)
    target_include_directories(trevrpc_wire_test PRIVATE src)
    target_compile_definitions(trevrpc_wire_test PRIVATE TREVRPC_WIRE_GOLDEN_VECTORS="${CMAKE_CURRENT_SOURCE_DIR}/../testdata/wire-golden-vectors.txt")
    target_link_libraries(trevrpc_wire_test PRIVATE trevrpc_core)
    trevrpc_c_warnings(trevrpc_wire_test)
    trevrpc_c_sanitizers(trevrpc_wire_test)

    add_test(NAME trevrpc_wire COMMAND trevrpc_wire_test)

    add_executable(trevrpc_ownership_test tests/ownership_test.c)
    target_include_directories(trevrpc_ownership_test PRIVATE src)
    target_link_libraries(trevrpc_ownership_test PRIVATE trevrpc_core)
    trevrpc_c_warnings(trevrpc_ownership_test)
    trevrpc_c_sanitizers(trevrpc_ownership_test)

    add_test(NAME trevrpc_ownership COMMAND trevrpc_ownership_test)

    if(TARGET trevrpc_bench_peer_c)
        add_test(NAME trevrpc_bench_peer_capabilities COMMAND trevrpc_bench_peer_c capabilities)
        set_tests_properties(trevrpc_bench_peer_capabilities PROPERTIES
            PASS_REGULAR_EXPRESSION
                "\"schema_version\":4.*\"roles\":\\{\"client\":\\[\"trevrpc_native_quic\",\"grpc_http2\"\\],\"server\":\\[\"trevrpc_native_quic\",\"grpc_http2\",\"trevrpc_webtransport\"\\]\\}.*\"rpc_kinds\":\\[\"unary\",\"client_stream\",\"server_stream\",\"bidi\"\\].*\"histogram\":\"log_linear_v1\""
        )
        add_test(NAME trevrpc_bench_peer_webtransport_config
            COMMAND "${CMAKE_COMMAND}"
                -DPEER=$<TARGET_FILE:trevrpc_bench_peer_c>
                -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/benchmark_peer_webtransport_config.cmake"
        )
        add_test(NAME trevrpc_bench_peer_webtransport_smoke
            COMMAND sh
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/benchmark_peer_webtransport_smoke.sh"
                "$<TARGET_FILE:trevrpc_bench_peer_c>"
                "${trevrpc_msquic_test_cert}"
                "${trevrpc_msquic_test_key}"
        )
        set_tests_properties(trevrpc_bench_peer_webtransport_smoke PROPERTIES TIMEOUT 30)
        trevrpc_c_lsan_suppressions(trevrpc_bench_peer_webtransport_smoke)
        add_test(NAME trevrpc_bench_peer_grpc_smoke
            COMMAND sh
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/benchmark_peer_grpc_smoke.sh"
                "$<TARGET_FILE:trevrpc_bench_peer_c>"
                "${trevrpc_msquic_test_cert}"
                "${trevrpc_msquic_test_key}"
        )
        set_tests_properties(trevrpc_bench_peer_grpc_smoke PROPERTIES TIMEOUT 60)
        trevrpc_c_lsan_suppressions(trevrpc_bench_peer_grpc_smoke)
        if(TREVRPC_ENABLE_SANITIZERS)
            # LeakSanitizer cannot reliably suspend gRPC's uninstrumented EventEngine workers.
            set_property(TEST trevrpc_bench_peer_grpc_smoke APPEND PROPERTY ENVIRONMENT "ASAN_OPTIONS=detect_leaks=0")
        endif()
    endif()

    if(TREVRPC_BUILD_RUNTIME)
        add_executable(trevrpc_api_test tests/abi6/public_api.c)
        target_link_libraries(trevrpc_api_test PRIVATE trevrpc)
        trevrpc_c_warnings(trevrpc_api_test)
        trevrpc_c_sanitizers(trevrpc_api_test)

        add_test(NAME trevrpc_api COMMAND trevrpc_api_test)

        add_executable(trevrpc_api_cpp_test tests/abi6/public_api.cpp)
        target_link_libraries(trevrpc_api_cpp_test PRIVATE trevrpc)
        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(trevrpc_api_cpp_test PRIVATE -Wall -Wextra -Werror)
        endif()
        trevrpc_c_sanitizers(trevrpc_api_cpp_test)

        add_test(NAME trevrpc_api_cpp COMMAND trevrpc_api_cpp_test)

        add_test(NAME trevrpc_abi6_symbols
            COMMAND "${CMAKE_COMMAND}"
                -DTREVRPC_NM=${CMAKE_NM}
                "-DTREVRPC_ARCHIVES=$<TARGET_FILE:trevrpc>;$<TARGET_FILE:trevrpc_core>;$<TARGET_FILE:trevrpc_msquic>;$<TARGET_FILE:trevrpc_webtransport>"
                "-DTREVRPC_PUBLIC_HEADERS=${CMAKE_CURRENT_SOURCE_DIR}/include/trevrpc.h;${CMAKE_CURRENT_SOURCE_DIR}/include/trevrpc_raw.h;${CMAKE_CURRENT_SOURCE_DIR}/include/trevrpc_binding.h;${CMAKE_CURRENT_SOURCE_DIR}/include/trevrpc_msquic.h;${CMAKE_CURRENT_SOURCE_DIR}/include/trevrpc_webtransport.h"
                -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/abi6/check_symbols.cmake"
        )

        add_executable(trevrpc_cancellation_lifecycle_test tests/cancellation_lifecycle_test.c)
        target_link_libraries(trevrpc_cancellation_lifecycle_test PRIVATE trevrpc)
        trevrpc_c_warnings(trevrpc_cancellation_lifecycle_test)
        trevrpc_c_sanitizers(trevrpc_cancellation_lifecycle_test)

        add_test(NAME trevrpc_cancellation_lifecycle COMMAND trevrpc_cancellation_lifecycle_test)

        add_executable(trevrpc_stream_lifecycle_test tests/stream_lifecycle_test.c)
        target_include_directories(trevrpc_stream_lifecycle_test PRIVATE src)
        target_link_libraries(trevrpc_stream_lifecycle_test PRIVATE trevrpc_testing)
        trevrpc_c_warnings(trevrpc_stream_lifecycle_test)
        trevrpc_c_sanitizers(trevrpc_stream_lifecycle_test)

        add_test(NAME trevrpc_stream_lifecycle COMMAND trevrpc_stream_lifecycle_test)

        add_executable(trevrpc_versioned_api_test tests/versioned_api_test.c)
        target_include_directories(trevrpc_versioned_api_test PRIVATE src)
        target_link_libraries(trevrpc_versioned_api_test PRIVATE trevrpc)
        trevrpc_c_warnings(trevrpc_versioned_api_test)
        trevrpc_c_sanitizers(trevrpc_versioned_api_test)

        add_test(NAME trevrpc_versioned_api COMMAND trevrpc_versioned_api_test)
        trevrpc_c_tsan_suppressions(trevrpc_versioned_api)

        add_executable(trevrpc_server_lifecycle_test tests/server_lifecycle_test.c)
        target_include_directories(trevrpc_server_lifecycle_test PRIVATE src)
        target_link_libraries(trevrpc_server_lifecycle_test PRIVATE trevrpc_testing)
        trevrpc_c_warnings(trevrpc_server_lifecycle_test)
        trevrpc_c_sanitizers(trevrpc_server_lifecycle_test)

        add_test(NAME trevrpc_server_lifecycle COMMAND trevrpc_server_lifecycle_test)
        set_tests_properties(trevrpc_server_lifecycle PROPERTIES TIMEOUT 30)

        add_executable(trevrpc_runtime_test tests/runtime_test.c)
        target_include_directories(trevrpc_runtime_test PRIVATE src)
        target_link_libraries(trevrpc_runtime_test PRIVATE trevrpc_testing)
        trevrpc_c_warnings(trevrpc_runtime_test)
        trevrpc_c_sanitizers(trevrpc_runtime_test)

        add_test(NAME trevrpc_runtime COMMAND trevrpc_runtime_test)

        add_executable(trevrpc_msquic_transport_test tests/msquic_transport_test.c)
        add_dependencies(trevrpc_msquic_transport_test trevrpc_msquic_test_certificates)
        target_include_directories(trevrpc_msquic_transport_test PRIVATE src)
        target_compile_definitions(trevrpc_msquic_transport_test PRIVATE
            TREVRPC_MSQUIC_TESTING
            TREVRPC_MSQUIC_TEST_CERT="${trevrpc_msquic_test_cert}"
            TREVRPC_MSQUIC_TEST_KEY="${trevrpc_msquic_test_key}"
        )
        target_link_libraries(trevrpc_msquic_transport_test PRIVATE trevrpc_msquic_testing_runtime)
        trevrpc_c_warnings(trevrpc_msquic_transport_test)
        trevrpc_c_sanitizers(trevrpc_msquic_transport_test)

        add_test(NAME trevrpc_msquic_transport COMMAND trevrpc_msquic_transport_test)
        trevrpc_c_lsan_suppressions(trevrpc_msquic_transport)
        trevrpc_c_tsan_suppressions(trevrpc_msquic_transport)

        add_executable(trevrpc_channel_test tests/channel_test.c)
        add_dependencies(trevrpc_channel_test trevrpc_msquic_test_certificates)
        target_compile_definitions(trevrpc_channel_test PRIVATE
            TREVRPC_MSQUIC_TEST_CERT="${trevrpc_msquic_test_cert}"
            TREVRPC_MSQUIC_TEST_KEY="${trevrpc_msquic_test_key}"
        )
        target_link_libraries(trevrpc_channel_test PRIVATE trevrpc)
        trevrpc_c_warnings(trevrpc_channel_test)
        trevrpc_c_sanitizers(trevrpc_channel_test)

        add_test(NAME trevrpc_channel COMMAND trevrpc_channel_test)
        set_tests_properties(trevrpc_channel PROPERTIES TIMEOUT 30)
        trevrpc_c_lsan_suppressions(trevrpc_channel)
        trevrpc_c_tsan_suppressions(trevrpc_channel)

        add_executable(trevrpc_buffering_config_test tests/buffering_config_test.c)
        target_include_directories(trevrpc_buffering_config_test PRIVATE src)
        target_link_libraries(trevrpc_buffering_config_test PRIVATE trevrpc_testing)
        trevrpc_c_warnings(trevrpc_buffering_config_test)
        trevrpc_c_sanitizers(trevrpc_buffering_config_test)

        add_test(NAME trevrpc_buffering_config COMMAND trevrpc_buffering_config_test)

        add_executable(trevrpc_buffering_memory_test tests/buffering_memory_test.c)
        add_dependencies(trevrpc_buffering_memory_test trevrpc_msquic_test_certificates)
        target_compile_definitions(trevrpc_buffering_memory_test PRIVATE
            TREVRPC_MSQUIC_TEST_CERT="${trevrpc_msquic_test_cert}"
            TREVRPC_MSQUIC_TEST_KEY="${trevrpc_msquic_test_key}"
        )
        target_link_libraries(trevrpc_buffering_memory_test PRIVATE trevrpc)
        trevrpc_c_warnings(trevrpc_buffering_memory_test)
        trevrpc_c_sanitizers(trevrpc_buffering_memory_test)

        set(trevrpc_buffering_fast_cases
            "safe-slow-reader|slow-reader|4|4096|16384|262144|65536|1"
            "safe-stalled-handler|stalled-handler|8|32768|4096|524288|0|10"
            "safe-reset|reset|8|32768|4096|262144|0|5"
            "safe-close|close|8|32768|8192|262144|65536|5"
            "safe-overload|overload|12|32768|4096|262144|0|10"
            "safe-body-limit|body-limit|4|32768|4096|262144|0|5"
        )
        foreach(trevrpc_buffering_case IN LISTS trevrpc_buffering_fast_cases)
            string(REPLACE "|" ";" trevrpc_buffering_args "${trevrpc_buffering_case}")
            list(POP_FRONT trevrpc_buffering_args trevrpc_buffering_name)
            add_test(NAME trevrpc_buffering_memory_${trevrpc_buffering_name}
                COMMAND trevrpc_buffering_memory_test ${trevrpc_buffering_args}
            )
            set_tests_properties(trevrpc_buffering_memory_${trevrpc_buffering_name}
                PROPERTIES LABELS "buffering" TIMEOUT 30
            )
            trevrpc_c_lsan_suppressions(trevrpc_buffering_memory_${trevrpc_buffering_name})
            trevrpc_c_tsan_suppressions(trevrpc_buffering_memory_${trevrpc_buffering_name})
        endforeach()

        add_executable(trevrpc_generated_service_test
            tests/generated_service_test.c
            "${trevrpc_generated_pb_c}"
            "${trevrpc_generated_trevrpc_c}"
        )
        add_dependencies(trevrpc_generated_service_test trevrpc_msquic_test_certificates)
        target_compile_definitions(trevrpc_generated_service_test PRIVATE
            TREVRPC_MSQUIC_TEST_CERT="${trevrpc_msquic_test_cert}"
            TREVRPC_MSQUIC_TEST_KEY="${trevrpc_msquic_test_key}"
            TREVRPC_GENERATED_HEADER="${trevrpc_generated_test_dir}/greeter.trevrpc.h"
            TREVRPC_GENERATED_SOURCE="${trevrpc_generated_test_dir}/greeter.trevrpc.c"
            TREVRPC_GENERATED_TESTING
        )
        target_include_directories(trevrpc_generated_service_test PRIVATE src "${trevrpc_generated_test_dir}")
        target_link_libraries(trevrpc_generated_service_test PRIVATE trevrpc_msquic_testing trevrpc_testing protobuf-c)
        trevrpc_c_warnings(trevrpc_generated_service_test)
        trevrpc_c_sanitizers(trevrpc_generated_service_test)

        add_test(NAME trevrpc_generated_service COMMAND trevrpc_generated_service_test)
        trevrpc_c_lsan_suppressions(trevrpc_generated_service)
        trevrpc_c_tsan_suppressions(trevrpc_generated_service)
        add_test(NAME trevrpc_codegen_golden_header
            COMMAND "${CMAKE_COMMAND}" -E compare_files
                "${trevrpc_generated_test_dir}/greeter.trevrpc.h"
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/golden/greeter.trevrpc.h"
        )
        add_test(NAME trevrpc_codegen_golden_source
            COMMAND "${CMAKE_COMMAND}" -E compare_files
                "${trevrpc_generated_test_dir}/greeter.trevrpc.c"
                "${CMAKE_CURRENT_SOURCE_DIR}/tests/golden/greeter.trevrpc.c"
        )
    endif()

endif()
