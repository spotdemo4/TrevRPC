cmake_minimum_required(VERSION 3.20)

project(trevrpc-c VERSION 0.2.2 LANGUAGES C)

set(TREVRPC_ENGINE_ABI_VERSION 1)
set(TREVRPC_ENGINE_MSQUIC_ABI_VERSION 1)
set(TREVRPC_TRANSPORT_ABI_VERSION 1)
set(TREVRPC_TRANSPORT_MSQUIC_ABI_VERSION 1)
set(TREVRPC_RPC_ABI_VERSION 1)
set(TREVRPC_RPC_MSQUIC_ABI_VERSION 1)

# Export compile_commands.json for clangd / editor integration. Honor a
# user-provided value but default to ON.
if(NOT DEFINED CMAKE_EXPORT_COMPILE_COMMANDS)
    set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
endif()

include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
include(CheckCSourceCompiles)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

set(TREVRPC_MSQUIC_MODULE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/provider/msquic")
set(TREVRPC_MSQUIC_SOURCE_DIR "${TREVRPC_MSQUIC_MODULE_DIR}/src")
set(TREVRPC_MSQUIC_INCLUDE_DIR "${TREVRPC_MSQUIC_MODULE_DIR}/include")

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
    set(trevrpc_default_build_codegen ON)
else()
    set(trevrpc_default_build_codegen OFF)
endif()
if(UNIX)
    set(trevrpc_default_build_engine ON)
else()
    set(trevrpc_default_build_engine OFF)
endif()

option(TREVRPC_BUILD_TESTS "Build trevrpc-c tests" ON)
option(TREVRPC_BUILD_BENCHMARKS "Build trevrpc-c benchmark executables" OFF)
option(TREVRPC_BUILD_EXAMPLES "Build trevrpc-c example executables" "${TREVRPC_BUILD_TESTS}")
option(TREVRPC_BUILD_ENGINE "Build the TrevRPC Engine ABI library" "${trevrpc_default_build_engine}")
option(TREVRPC_BUILD_TRANSPORT "Build the provider-neutral TrevRPC transport ABI library" "${trevrpc_default_build_engine}")
option(TREVRPC_BUILD_RPC "Build the asynchronous TrevRPC RPC ABI library" "${trevrpc_default_build_engine}")
option(TREVRPC_BUILD_ENGINE_MSQUIC
    "Build the independently versioned MsQuic native engine provider"
    "${trevrpc_default_build_engine}"
)
option(TREVRPC_BUILD_TRANSPORT_MSQUIC
    "Build the MsQuic native, HTTP/3, and WebTransport transport companion"
    "${TREVRPC_BUILD_ENGINE_MSQUIC}"
)
option(TREVRPC_BUILD_RPC_MSQUIC "Build the MsQuic RPC runtime companion" "${TREVRPC_BUILD_TRANSPORT_MSQUIC}")
option(TREVRPC_BUILD_CODEGEN "Build protoc-gen-trevrpc-c" "${trevrpc_default_build_codegen}")
set(trevrpc_default_build_rpc_test_support OFF)
if(TREVRPC_BUILD_TESTS AND TREVRPC_BUILD_RPC)
    set(trevrpc_default_build_rpc_test_support ON)
endif()
option(TREVRPC_BUILD_RPC_TEST_SUPPORT
    "Build the private RPC fake transport support library"
    "${trevrpc_default_build_rpc_test_support}"
)
option(TREVRPC_ENABLE_SANITIZERS "Build trevrpc-c with AddressSanitizer and UndefinedBehaviorSanitizer" OFF)
option(TREVRPC_ENABLE_THREAD_SANITIZER "Build trevrpc-c with ThreadSanitizer" OFF)
if(TREVRPC_BUILD_ENGINE AND NOT UNIX)
    message(FATAL_ERROR "TREVRPC_BUILD_ENGINE requires a POSIX platform with file descriptors and pthreads")
endif()
if(TREVRPC_BUILD_ENGINE_MSQUIC AND NOT TREVRPC_BUILD_ENGINE)
    message(FATAL_ERROR "TREVRPC_BUILD_ENGINE_MSQUIC requires TREVRPC_BUILD_ENGINE")
endif()
if(TREVRPC_BUILD_ENGINE_MSQUIC AND NOT UNIX)
    message(FATAL_ERROR "TREVRPC_BUILD_ENGINE_MSQUIC requires a POSIX platform with file descriptors and pthreads")
endif()
if(TREVRPC_BUILD_TRANSPORT AND NOT UNIX)
    message(FATAL_ERROR "TREVRPC_BUILD_TRANSPORT requires a POSIX platform with file descriptors and pthreads")
endif()
if(TREVRPC_BUILD_TRANSPORT_MSQUIC AND NOT TREVRPC_BUILD_TRANSPORT)
    message(FATAL_ERROR "TREVRPC_BUILD_TRANSPORT_MSQUIC requires TREVRPC_BUILD_TRANSPORT")
endif()
if(TREVRPC_BUILD_TRANSPORT_MSQUIC AND NOT TREVRPC_BUILD_ENGINE_MSQUIC)
    message(FATAL_ERROR "TREVRPC_BUILD_TRANSPORT_MSQUIC requires TREVRPC_BUILD_ENGINE_MSQUIC")
endif()
if(TREVRPC_BUILD_RPC_MSQUIC AND NOT TREVRPC_BUILD_RPC)
    message(FATAL_ERROR "TREVRPC_BUILD_RPC_MSQUIC requires TREVRPC_BUILD_RPC")
endif()
if(TREVRPC_BUILD_RPC_MSQUIC AND NOT TREVRPC_BUILD_TRANSPORT_MSQUIC)
    message(FATAL_ERROR "TREVRPC_BUILD_RPC_MSQUIC requires TREVRPC_BUILD_TRANSPORT_MSQUIC")
endif()
if(TREVRPC_BUILD_RPC_TEST_SUPPORT AND NOT TREVRPC_BUILD_RPC)
    message(FATAL_ERROR "TREVRPC_BUILD_RPC_TEST_SUPPORT requires TREVRPC_BUILD_RPC")
endif()
if(TREVRPC_ENABLE_SANITIZERS AND TREVRPC_ENABLE_THREAD_SANITIZER)
    message(FATAL_ERROR "TREVRPC_ENABLE_SANITIZERS and TREVRPC_ENABLE_THREAD_SANITIZER are mutually exclusive")
endif()
set(TREVRPC_ENGINE_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/trevrpc_engine" CACHE PATH
    "Install directory for trevrpc_engine CMake package files")
set(TREVRPC_ENGINE_MSQUIC_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/trevrpc_engine_msquic" CACHE PATH
    "Install directory for trevrpc_engine_msquic CMake package files")
set(TREVRPC_TRANSPORT_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/trevrpc_transport" CACHE PATH
    "Install directory for trevrpc_transport CMake package files")
set(TREVRPC_TRANSPORT_MSQUIC_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/trevrpc_transport_msquic" CACHE PATH
    "Install directory for trevrpc_transport_msquic CMake package files")
set(TREVRPC_TRANSPORT_PRIVATE_INSTALL_LIBDIR "${CMAKE_INSTALL_LIBDIR}/trevrpc/transport-abi1" CACHE PATH
    "Install directory for private transport implementation archives")
set(TREVRPC_RPC_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/trevrpc_rpc" CACHE PATH
    "Install directory for trevrpc_rpc CMake package files")
set(TREVRPC_RPC_MSQUIC_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/trevrpc_rpc_msquic" CACHE PATH
    "Install directory for trevrpc_rpc_msquic CMake package files")
set(TREVRPC_RPC_PRIVATE_INSTALL_LIBDIR "${CMAKE_INSTALL_LIBDIR}/trevrpc/rpc-abi1" CACHE PATH
    "Install directory for private RPC implementation archives")
set(TREVRPC_INSTALL_PKGCONFIGDIR "${CMAKE_INSTALL_LIBDIR}/pkgconfig" CACHE STRING
    "Install directory for trevrpc pkg-config files")

find_package(Threads REQUIRED)

if(TREVRPC_BUILD_ENGINE)
    check_c_source_compiles([=[
#define _GNU_SOURCE
#include <fcntl.h>
#include <unistd.h>
int main(void) {
    int descriptors[2];
    return pipe2(descriptors, O_NONBLOCK | O_CLOEXEC);
}
]=] TREVRPC_ENGINE_HAVE_PIPE2)
endif()

if(TREVRPC_BUILD_RPC OR TREVRPC_BUILD_TRANSPORT_MSQUIC)
    add_library(trevrpc_core STATIC
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_frame.c
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_values.c
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_wire.c
    )
    target_include_directories(trevrpc_core
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${TREVRPC_MSQUIC_SOURCE_DIR}>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE src
    )
endif()

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

function(trevrpc_c_msquic_loopback_test test_name)
    set_tests_properties(${test_name} PROPERTIES
        RESOURCE_LOCK trevrpc_msquic_loopback
    )
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

if(TARGET trevrpc_core)
    trevrpc_c_warnings(trevrpc_core)
    trevrpc_c_sanitizers(trevrpc_core)
endif()

if(TREVRPC_BUILD_RPC)
    add_library(trevrpc_rpc_private_core STATIC
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_values.c
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_wire.c
    )
    target_include_directories(trevrpc_rpc_private_core PRIVATE include src)
    trevrpc_c_warnings(trevrpc_rpc_private_core)
    trevrpc_c_sanitizers(trevrpc_rpc_private_core)
endif()

if(TREVRPC_BUILD_ENGINE)
    add_library(trevrpc_engine STATIC src/trevrpc_engine.c)
    target_include_directories(trevrpc_engine
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE src
    )
    target_link_libraries(trevrpc_engine PUBLIC Threads::Threads)
    if(TREVRPC_ENGINE_HAVE_PIPE2)
        target_compile_definitions(trevrpc_engine PRIVATE TREVRPC_ENGINE_HAVE_PIPE2)
    endif()
    trevrpc_c_warnings(trevrpc_engine)
    trevrpc_c_sanitizers(trevrpc_engine)

    if(TREVRPC_BUILD_TESTS)
        add_library(trevrpc_engine_testing STATIC src/trevrpc_engine_test_support.c)
        target_include_directories(trevrpc_engine_testing
            PUBLIC include
            PRIVATE src
        )
        target_link_libraries(trevrpc_engine_testing PUBLIC trevrpc_engine)
        trevrpc_c_warnings(trevrpc_engine_testing)
        trevrpc_c_sanitizers(trevrpc_engine_testing)

        add_library(trevrpc_credential_testing STATIC src/trevrpc_credential_testing.c)
        target_include_directories(trevrpc_credential_testing PRIVATE src)
        target_compile_definitions(trevrpc_credential_testing PRIVATE TREVRPC_CREDENTIAL_TESTING)
        trevrpc_c_warnings(trevrpc_credential_testing)
        trevrpc_c_sanitizers(trevrpc_credential_testing)
    endif()
endif()

if(TREVRPC_BUILD_TRANSPORT)
    add_library(trevrpc_transport STATIC src/trevrpc_transport.c)
    target_include_directories(trevrpc_transport
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE src
    )
    trevrpc_c_warnings(trevrpc_transport)
    trevrpc_c_sanitizers(trevrpc_transport)

    if(TREVRPC_BUILD_TESTS)
        add_library(trevrpc_transport_testing STATIC tests/trevrpc_transport_testing.c)
        target_include_directories(trevrpc_transport_testing
            PUBLIC tests
            PRIVATE
                src
                ${TREVRPC_MSQUIC_INCLUDE_DIR}
                ${TREVRPC_MSQUIC_SOURCE_DIR}
        )
        target_link_libraries(trevrpc_transport_testing PUBLIC trevrpc_transport Threads::Threads)
        trevrpc_c_warnings(trevrpc_transport_testing)
        trevrpc_c_sanitizers(trevrpc_transport_testing)
        configure_file(
            cmake/trevrpc_transport_testing.pc.in
            "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_transport_testing.pc"
            @ONLY
        )
    endif()
endif()

if(TREVRPC_BUILD_RPC)
    add_library(trevrpc_rpc STATIC
        src/trevrpc_rpc.c
        src/trevrpc_rpc_runtime.c
    )
    target_include_directories(trevrpc_rpc
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE src ${TREVRPC_MSQUIC_SOURCE_DIR}
    )
    target_link_libraries(trevrpc_rpc
        PUBLIC Threads::Threads
        PRIVATE
            $<BUILD_INTERFACE:trevrpc_rpc_private_core>
            $<INSTALL_INTERFACE:-ltrevrpc_rpc_private_core>
    )
    target_link_directories(trevrpc_rpc INTERFACE
        $<INSTALL_INTERFACE:${TREVRPC_RPC_PRIVATE_INSTALL_LIBDIR}>
    )
    trevrpc_c_warnings(trevrpc_rpc)
    trevrpc_c_sanitizers(trevrpc_rpc)

endif()

if(TREVRPC_BUILD_RPC_TEST_SUPPORT)
    # Test/conformance-only fake transport support; deliberately not installed.
    add_library(trevrpc_rpc_fake_transport_support STATIC tests/rpc_fake_transport_support.c)
    target_include_directories(
        trevrpc_rpc_fake_transport_support PRIVATE src tests ${TREVRPC_MSQUIC_SOURCE_DIR})
    target_link_libraries(trevrpc_rpc_fake_transport_support PUBLIC trevrpc_rpc)
    trevrpc_c_warnings(trevrpc_rpc_fake_transport_support)
    trevrpc_c_sanitizers(trevrpc_rpc_fake_transport_support)
endif()

set(trevrpc_pkg_config_files)

if(IS_ABSOLUTE "${TREVRPC_INSTALL_PKGCONFIGDIR}")
    set(trevrpc_pc_full_pkgconfigdir "${TREVRPC_INSTALL_PKGCONFIGDIR}")
else()
    set(trevrpc_pc_full_pkgconfigdir
        "${CMAKE_INSTALL_PREFIX}/${TREVRPC_INSTALL_PKGCONFIGDIR}"
    )
endif()
file(RELATIVE_PATH trevrpc_pc_prefix_from_pc
    "${trevrpc_pc_full_pkgconfigdir}"
    "${CMAKE_INSTALL_PREFIX}"
)
file(RELATIVE_PATH trevrpc_pc_lib_from_pc
    "${trevrpc_pc_full_pkgconfigdir}"
    "${CMAKE_INSTALL_FULL_LIBDIR}"
)
file(RELATIVE_PATH trevrpc_pc_include_from_pc
    "${trevrpc_pc_full_pkgconfigdir}"
    "${CMAKE_INSTALL_FULL_INCLUDEDIR}"
)
if(IS_ABSOLUTE "${TREVRPC_TRANSPORT_PRIVATE_INSTALL_LIBDIR}")
    set(trevrpc_transport_private_full_libdir "${TREVRPC_TRANSPORT_PRIVATE_INSTALL_LIBDIR}")
else()
    set(trevrpc_transport_private_full_libdir
        "${CMAKE_INSTALL_PREFIX}/${TREVRPC_TRANSPORT_PRIVATE_INSTALL_LIBDIR}"
    )
endif()
file(RELATIVE_PATH trevrpc_transport_private_lib_from_pc
    "${trevrpc_pc_full_pkgconfigdir}"
    "${trevrpc_transport_private_full_libdir}"
)
set(trevrpc_pc_prefix "\${pcfiledir}/${trevrpc_pc_prefix_from_pc}")
set(trevrpc_pc_libdir "\${pcfiledir}/${trevrpc_pc_lib_from_pc}")
set(trevrpc_pc_includedir "\${pcfiledir}/${trevrpc_pc_include_from_pc}")
set(trevrpc_transport_private_pc_libdir
    "\${pcfiledir}/${trevrpc_transport_private_lib_from_pc}"
)

if(TREVRPC_BUILD_ENGINE)
    list(APPEND trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_engine.pc")
    configure_file(cmake/trevrpc_engine.pc.in "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_engine.pc" @ONLY)
endif()
if(TREVRPC_BUILD_TRANSPORT)
    list(APPEND trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_transport.pc")
    configure_file(cmake/trevrpc_transport.pc.in "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_transport.pc" @ONLY)
endif()
if(TREVRPC_BUILD_RPC)
    list(APPEND trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_rpc.pc")
    configure_file(cmake/trevrpc_rpc.pc.in "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_rpc.pc" @ONLY)
endif()

if(TREVRPC_BUILD_ENGINE_MSQUIC)
    find_package(msquic CONFIG REQUIRED)

    add_library(trevrpc_msquic_api_owner STATIC ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_msquic_api_owner.c)
    target_include_directories(trevrpc_msquic_api_owner PRIVATE src)
    target_link_libraries(trevrpc_msquic_api_owner PRIVATE msquic::msquic Threads::Threads)
    trevrpc_c_warnings(trevrpc_msquic_api_owner)
    trevrpc_c_sanitizers(trevrpc_msquic_api_owner)
endif()

if(TREVRPC_BUILD_TRANSPORT_MSQUIC)
    add_library(trevrpc_protocol_core STATIC
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_quic_varint.c
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_qpack_static.c
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_http3_frame.c
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_http3_headers.c
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_http3_settings.c
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_h3_demux.c
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_h3_ingress.c
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_msquic_features.c
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_webtransport_profile.c
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_webtransport_capsule.c
    )
    target_include_directories(trevrpc_protocol_core PRIVATE include src)
    target_link_libraries(trevrpc_protocol_core PRIVATE
        trevrpc_core
        trevrpc_msquic_api_owner
        msquic::msquic
        Threads::Threads
    )
    target_compile_definitions(trevrpc_protocol_core PRIVATE
        QUIC_API_ENABLE_PREVIEW_FEATURES
        QUIC_API_ENABLE_VERSIONED_FEATURES=1
        TREVRPC_MSQUIC_PROVIDER_CAPABILITIES
    )
    trevrpc_c_warnings(trevrpc_protocol_core)
    trevrpc_c_sanitizers(trevrpc_protocol_core)

    add_library(trevrpc_msquic_native_core STATIC
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_msquic_core.c
    )
    target_include_directories(trevrpc_msquic_native_core PRIVATE include src)
    target_link_libraries(trevrpc_msquic_native_core
        PUBLIC Threads::Threads trevrpc_core
        PRIVATE
            $<BUILD_INTERFACE:trevrpc_protocol_core>
            $<INSTALL_INTERFACE:-ltrevrpc_protocol_core>
            trevrpc_msquic_api_owner
            msquic::msquic
    )
    target_link_directories(trevrpc_msquic_native_core INTERFACE
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_LIBDIR}>
    )
    target_compile_definitions(trevrpc_msquic_native_core PRIVATE
        QUIC_API_ENABLE_PREVIEW_FEATURES
        QUIC_API_ENABLE_VERSIONED_FEATURES=1
        TREVRPC_MSQUIC_PROVIDER_CAPABILITIES
    )
    trevrpc_c_warnings(trevrpc_msquic_native_core)
    trevrpc_c_sanitizers(trevrpc_msquic_native_core)
    if(TREVRPC_BUILD_TESTS)
        add_library(trevrpc_msquic_testing STATIC
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_msquic_core.c
        )
        target_compile_definitions(trevrpc_msquic_testing PRIVATE
            QUIC_API_ENABLE_VERSIONED_FEATURES=1
            TREVRPC_MSQUIC_PROVIDER_CAPABILITIES
            TREVRPC_MSQUIC_TESTING
        )
        trevrpc_c_configure_msquic(trevrpc_msquic_testing)
        target_include_directories(trevrpc_msquic_testing PRIVATE src)
        target_link_libraries(trevrpc_msquic_testing PRIVATE trevrpc_protocol_core trevrpc_msquic_api_owner)
    endif()

endif()

if(TREVRPC_BUILD_ENGINE_MSQUIC)
    add_library(trevrpc_engine_msquic_support STATIC ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_frame.c)
    target_include_directories(trevrpc_engine_msquic_support PRIVATE include src)
    trevrpc_c_warnings(trevrpc_engine_msquic_support)
    trevrpc_c_sanitizers(trevrpc_engine_msquic_support)

    add_library(trevrpc_engine_msquic STATIC ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_engine_msquic.c)
    target_include_directories(trevrpc_engine_msquic
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${TREVRPC_MSQUIC_INCLUDE_DIR}>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE src
    )
    target_link_libraries(trevrpc_engine_msquic
        PUBLIC trevrpc_engine
        PRIVATE
            Threads::Threads
            $<BUILD_INTERFACE:trevrpc_msquic_api_owner>
            $<BUILD_INTERFACE:trevrpc_engine_msquic_support>
            $<INSTALL_INTERFACE:-ltrevrpc_msquic_api_owner>
            $<INSTALL_INTERFACE:-ltrevrpc_engine_msquic_support>
            msquic::msquic
    )
    target_link_directories(trevrpc_engine_msquic INTERFACE
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_LIBDIR}>
    )
    target_compile_definitions(trevrpc_engine_msquic PRIVATE QUIC_API_ENABLE_PREVIEW_FEATURES)
    trevrpc_c_warnings(trevrpc_engine_msquic)
    trevrpc_c_sanitizers(trevrpc_engine_msquic)

    list(APPEND trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_engine_msquic.pc")
    configure_file(cmake/trevrpc_engine_msquic.pc.in
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_engine_msquic.pc" @ONLY)
endif()

if(TREVRPC_BUILD_TRANSPORT_MSQUIC)
    add_library(trevrpc_transport_msquic_private STATIC
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_rpc_transport_engine.c
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_rpc_transport_h3.c
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_rpc_transport_msquic.c
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_rpc_transport_provider_adapter.c
    )
    target_include_directories(trevrpc_transport_msquic_private PRIVATE include src)
    target_link_libraries(trevrpc_transport_msquic_private PRIVATE
        trevrpc_engine_msquic
        trevrpc_msquic_native_core
        trevrpc_protocol_core
        trevrpc_msquic_api_owner
        msquic::msquic
        Threads::Threads
    )
    trevrpc_c_warnings(trevrpc_transport_msquic_private)
    trevrpc_c_sanitizers(trevrpc_transport_msquic_private)

    if(TREVRPC_BUILD_TESTS)
        add_library(trevrpc_transport_msquic_private_testing STATIC
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_rpc_transport_engine.c
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_rpc_transport_h3.c
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_rpc_transport_msquic.c
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_rpc_transport_provider_adapter.c
        )
        target_include_directories(trevrpc_transport_msquic_private_testing PRIVATE include src)
        target_compile_definitions(trevrpc_transport_msquic_private_testing PRIVATE TREVRPC_CREDENTIAL_TESTING)
        target_link_libraries(trevrpc_transport_msquic_private_testing PRIVATE
            trevrpc_credential_testing
            trevrpc_engine_msquic
            trevrpc_msquic_native_core
            trevrpc_protocol_core
            trevrpc_msquic_api_owner
            msquic::msquic
            Threads::Threads
        )
        trevrpc_c_warnings(trevrpc_transport_msquic_private_testing)
        trevrpc_c_sanitizers(trevrpc_transport_msquic_private_testing)
    endif()

    add_library(trevrpc_transport_msquic STATIC ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_transport_msquic.c)
    target_include_directories(trevrpc_transport_msquic
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${TREVRPC_MSQUIC_INCLUDE_DIR}>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE src
    )
    target_link_libraries(trevrpc_transport_msquic
        PUBLIC trevrpc_transport trevrpc_engine_msquic
        PRIVATE
            $<BUILD_INTERFACE:trevrpc_transport_msquic_private>
            $<BUILD_INTERFACE:trevrpc_msquic_native_core>
            $<BUILD_INTERFACE:trevrpc_protocol_core>
            $<BUILD_INTERFACE:trevrpc_msquic_api_owner>
            $<INSTALL_INTERFACE:-ltrevrpc_transport_msquic_private>
            $<INSTALL_INTERFACE:-ltrevrpc_transport_private_msquic_native>
            $<INSTALL_INTERFACE:-ltrevrpc_transport_private_protocol>
            $<INSTALL_INTERFACE:-ltrevrpc_transport_private_core>
            $<INSTALL_INTERFACE:-ltrevrpc_msquic_api_owner>
            msquic::msquic
    )
    target_link_directories(trevrpc_transport_msquic INTERFACE
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_LIBDIR}>
        $<INSTALL_INTERFACE:${TREVRPC_TRANSPORT_PRIVATE_INSTALL_LIBDIR}>
    )
    trevrpc_c_warnings(trevrpc_transport_msquic)
    trevrpc_c_sanitizers(trevrpc_transport_msquic)

    if(TREVRPC_BUILD_TESTS)
        add_library(trevrpc_transport_msquic_testing STATIC ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_transport_msquic.c)
        target_include_directories(trevrpc_transport_msquic_testing PRIVATE include src)
        target_link_libraries(trevrpc_transport_msquic_testing PUBLIC trevrpc_transport trevrpc_engine_msquic
            PRIVATE trevrpc_transport_msquic_private_testing trevrpc_msquic_native_core trevrpc_protocol_core
                trevrpc_msquic_api_owner msquic::msquic)
        trevrpc_c_warnings(trevrpc_transport_msquic_testing)
        trevrpc_c_sanitizers(trevrpc_transport_msquic_testing)
    endif()

    list(APPEND trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_transport_msquic.pc")
    configure_file(cmake/trevrpc_transport_msquic.pc.in
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_transport_msquic.pc" @ONLY)
endif()

if(TREVRPC_BUILD_RPC_MSQUIC)
    add_library(trevrpc_rpc_msquic STATIC ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_rpc_msquic.c)
    target_include_directories(trevrpc_rpc_msquic
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<BUILD_INTERFACE:${TREVRPC_MSQUIC_INCLUDE_DIR}>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE src ${TREVRPC_MSQUIC_SOURCE_DIR}
    )
    target_link_libraries(trevrpc_rpc_msquic PUBLIC trevrpc_rpc trevrpc_transport_msquic)
    trevrpc_c_warnings(trevrpc_rpc_msquic)
    trevrpc_c_sanitizers(trevrpc_rpc_msquic)

    if(TREVRPC_BUILD_TESTS)
        add_library(trevrpc_rpc_msquic_testing STATIC ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_rpc_msquic.c)
        target_include_directories(
            trevrpc_rpc_msquic_testing PRIVATE include src ${TREVRPC_MSQUIC_SOURCE_DIR})
        target_link_libraries(trevrpc_rpc_msquic_testing PUBLIC trevrpc_rpc trevrpc_transport_msquic_testing)
        trevrpc_c_warnings(trevrpc_rpc_msquic_testing)
        trevrpc_c_sanitizers(trevrpc_rpc_msquic_testing)
    endif()

    list(APPEND trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_rpc_msquic.pc")
    configure_file(cmake/trevrpc_rpc_msquic.pc.in
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_rpc_msquic.pc" @ONLY)
endif()

set(trevrpc_needs_generated_test_bindings OFF)
if(TREVRPC_BUILD_RPC_MSQUIC AND TREVRPC_BUILD_TESTS)
    set(trevrpc_needs_generated_test_bindings ON)
endif()
set(trevrpc_needs_generated_benchmark_bindings OFF)
if(TREVRPC_BUILD_BENCHMARKS AND TREVRPC_BUILD_RPC_MSQUIC)
    set(trevrpc_needs_generated_benchmark_bindings ON)
endif()
set(trevrpc_needs_generated_bindings OFF)
if(TREVRPC_BUILD_CODEGEN OR trevrpc_needs_generated_test_bindings
    OR trevrpc_needs_generated_benchmark_bindings
    OR (TREVRPC_BUILD_RPC_MSQUIC AND TREVRPC_BUILD_EXAMPLES))
    set(trevrpc_needs_generated_bindings ON)
endif()

if((TREVRPC_BUILD_ENGINE_MSQUIC AND TREVRPC_BUILD_TESTS)
    OR (TREVRPC_BUILD_RPC_MSQUIC AND (TREVRPC_BUILD_EXAMPLES OR TREVRPC_BUILD_BENCHMARKS)))
    find_program(OPENSSL_EXECUTABLE openssl REQUIRED)
    set(trevrpc_msquic_test_pki_config "${CMAKE_CURRENT_SOURCE_DIR}/tests/msquic-test-pki.cnf")
    set(trevrpc_msquic_test_ca_cert "${CMAKE_CURRENT_BINARY_DIR}/msquic-test-ca-cert.pem")
    set(trevrpc_msquic_test_ca_key "${CMAKE_CURRENT_BINARY_DIR}/msquic-test-ca-key.pem")
    set(trevrpc_msquic_test_cert "${CMAKE_CURRENT_BINARY_DIR}/msquic-test-cert.pem")
    set(trevrpc_msquic_test_csr "${CMAKE_CURRENT_BINARY_DIR}/msquic-test.csr")
    set(trevrpc_msquic_test_key "${CMAKE_CURRENT_BINARY_DIR}/msquic-test-key.pem")
    add_custom_command(
        OUTPUT
            "${trevrpc_msquic_test_ca_cert}"
            "${trevrpc_msquic_test_ca_key}"
            "${trevrpc_msquic_test_cert}"
            "${trevrpc_msquic_test_csr}"
            "${trevrpc_msquic_test_key}"
        COMMAND ${OPENSSL_EXECUTABLE} req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 3650 -subj "/CN=TrevRPC test root CA" -config "${trevrpc_msquic_test_pki_config}" -extensions ca_extensions -keyout "${trevrpc_msquic_test_ca_key}" -out "${trevrpc_msquic_test_ca_cert}"
        COMMAND ${OPENSSL_EXECUTABLE} req -new -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -subj /CN=localhost -keyout "${trevrpc_msquic_test_key}" -out "${trevrpc_msquic_test_csr}"
        COMMAND ${OPENSSL_EXECUTABLE} x509 -req -in "${trevrpc_msquic_test_csr}" -CA "${trevrpc_msquic_test_ca_cert}" -CAkey "${trevrpc_msquic_test_ca_key}" -set_serial 1 -days 3650 -extfile "${trevrpc_msquic_test_pki_config}" -extensions endpoint_extensions -out "${trevrpc_msquic_test_cert}"
        DEPENDS "${trevrpc_msquic_test_pki_config}"
        VERBATIM
    )
    add_custom_target(trevrpc_msquic_test_certificates
        DEPENDS
            "${trevrpc_msquic_test_ca_cert}"
            "${trevrpc_msquic_test_cert}"
            "${trevrpc_msquic_test_key}"
    )
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

    if(trevrpc_needs_generated_test_bindings)
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
    endif()

    if(trevrpc_needs_generated_benchmark_bindings)
        set(trevrpc_generated_test_dir "${CMAKE_CURRENT_BINARY_DIR}/generated-service-test")
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
        target_link_libraries(trevrpc_benchmark_generated PUBLIC trevrpc_rpc protobuf-c)
        trevrpc_c_warnings(trevrpc_benchmark_generated)
        trevrpc_c_sanitizers(trevrpc_benchmark_generated)
    endif()
endif()

if(TREVRPC_BUILD_RPC_MSQUIC AND TREVRPC_BUILD_EXAMPLES)
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
    target_link_libraries(trevrpc_greeter_example_generated PUBLIC trevrpc_rpc protobuf-c)
    trevrpc_c_warnings(trevrpc_greeter_example_generated)
    trevrpc_c_sanitizers(trevrpc_greeter_example_generated)

    add_executable(trevrpc_greeter_server examples/greeter/server.c)
    target_link_libraries(trevrpc_greeter_server PRIVATE
        trevrpc_greeter_example_generated
        trevrpc_rpc_msquic
    )
    trevrpc_c_warnings(trevrpc_greeter_server)
    trevrpc_c_sanitizers(trevrpc_greeter_server)

    add_executable(trevrpc_greeter_client examples/greeter/client.c)
    target_link_libraries(trevrpc_greeter_client PRIVATE
        trevrpc_greeter_example_generated
        trevrpc_rpc_msquic
    )
    trevrpc_c_warnings(trevrpc_greeter_client)
    trevrpc_c_sanitizers(trevrpc_greeter_client)
endif()

if(TREVRPC_BUILD_BENCHMARKS AND TREVRPC_BUILD_RPC_MSQUIC)
    find_package(OpenSSL REQUIRED COMPONENTS Crypto)

    add_executable(trevrpc_bench_peer_c bench/trevrpc_bench_peer.c)
    add_dependencies(trevrpc_bench_peer_c trevrpc_msquic_test_certificates)
    set_target_properties(trevrpc_bench_peer_c PROPERTIES OUTPUT_NAME "trevrpc-bench-peer-c")
    target_link_libraries(trevrpc_bench_peer_c PRIVATE
        trevrpc_benchmark_generated
        trevrpc_rpc_msquic
        OpenSSL::Crypto
    )
    trevrpc_c_warnings(trevrpc_bench_peer_c)
    trevrpc_c_sanitizers(trevrpc_bench_peer_c)
endif()

if(TREVRPC_BUILD_ENGINE)
    install(TARGETS trevrpc_engine
        EXPORT trevrpcEngineTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
    install(EXPORT trevrpcEngineTargets
        NAMESPACE trevrpc::
        DESTINATION "${TREVRPC_ENGINE_INSTALL_CMAKEDIR}"
    )
    configure_package_config_file(
        cmake/trevrpc_engineConfig.cmake.in
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_engineConfig.cmake"
        INSTALL_DESTINATION "${TREVRPC_ENGINE_INSTALL_CMAKEDIR}"
    )
    write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_engineConfigVersion.cmake"
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
    )
    install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_engineConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_engineConfigVersion.cmake"
        DESTINATION "${TREVRPC_ENGINE_INSTALL_CMAKEDIR}"
    )
endif()

if(TREVRPC_BUILD_ENGINE_MSQUIC)
    install(TARGETS trevrpc_engine_msquic
        EXPORT trevrpcEngineMsquicTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
    install(TARGETS trevrpc_engine_msquic_support
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
    install(TARGETS trevrpc_msquic_api_owner
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
    install(EXPORT trevrpcEngineMsquicTargets
        NAMESPACE trevrpc::
        DESTINATION "${TREVRPC_ENGINE_MSQUIC_INSTALL_CMAKEDIR}"
    )
    configure_package_config_file(
        cmake/trevrpc_engine_msquicConfig.cmake.in
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_engine_msquicConfig.cmake"
        INSTALL_DESTINATION "${TREVRPC_ENGINE_MSQUIC_INSTALL_CMAKEDIR}"
    )
    write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_engine_msquicConfigVersion.cmake"
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
    )
    install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_engine_msquicConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_engine_msquicConfigVersion.cmake"
        DESTINATION "${TREVRPC_ENGINE_MSQUIC_INSTALL_CMAKEDIR}"
    )
endif()

if(TREVRPC_BUILD_TRANSPORT)
    install(TARGETS trevrpc_transport
        EXPORT trevrpcTransportTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
    install(EXPORT trevrpcTransportTargets
        NAMESPACE trevrpc::
        DESTINATION "${TREVRPC_TRANSPORT_INSTALL_CMAKEDIR}"
    )
    configure_package_config_file(
        cmake/trevrpc_transportConfig.cmake.in
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_transportConfig.cmake"
        INSTALL_DESTINATION "${TREVRPC_TRANSPORT_INSTALL_CMAKEDIR}"
    )
    write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_transportConfigVersion.cmake"
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
    )
    install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_transportConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_transportConfigVersion.cmake"
        DESTINATION "${TREVRPC_TRANSPORT_INSTALL_CMAKEDIR}"
    )
endif()

if(TREVRPC_BUILD_TRANSPORT_MSQUIC)
    install(TARGETS trevrpc_transport_msquic
        EXPORT trevrpcTransportMsquicTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
    install(TARGETS trevrpc_transport_msquic_private
        ARCHIVE DESTINATION ${TREVRPC_TRANSPORT_PRIVATE_INSTALL_LIBDIR}
    )
    install(FILES $<TARGET_FILE:trevrpc_msquic_native_core>
        DESTINATION ${TREVRPC_TRANSPORT_PRIVATE_INSTALL_LIBDIR}
        RENAME "${CMAKE_STATIC_LIBRARY_PREFIX}trevrpc_transport_private_msquic_native${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    install(FILES $<TARGET_FILE:trevrpc_protocol_core>
        DESTINATION ${TREVRPC_TRANSPORT_PRIVATE_INSTALL_LIBDIR}
        RENAME "${CMAKE_STATIC_LIBRARY_PREFIX}trevrpc_transport_private_protocol${CMAKE_STATIC_LIBRARY_SUFFIX}"
    )
    install(EXPORT trevrpcTransportMsquicTargets
        NAMESPACE trevrpc::
        DESTINATION "${TREVRPC_TRANSPORT_MSQUIC_INSTALL_CMAKEDIR}"
    )
    configure_package_config_file(
        cmake/trevrpc_transport_msquicConfig.cmake.in
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_transport_msquicConfig.cmake"
        INSTALL_DESTINATION "${TREVRPC_TRANSPORT_MSQUIC_INSTALL_CMAKEDIR}"
    )
    write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_transport_msquicConfigVersion.cmake"
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
    )
    install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_transport_msquicConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_transport_msquicConfigVersion.cmake"
        DESTINATION "${TREVRPC_TRANSPORT_MSQUIC_INSTALL_CMAKEDIR}"
    )
endif()

if(TREVRPC_BUILD_RPC)
    install(TARGETS trevrpc_rpc
        EXPORT trevrpcRpcTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
    install(EXPORT trevrpcRpcTargets
        NAMESPACE trevrpc::
        DESTINATION "${TREVRPC_RPC_INSTALL_CMAKEDIR}"
    )
    configure_package_config_file(
        cmake/trevrpc_rpcConfig.cmake.in
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_rpcConfig.cmake"
        INSTALL_DESTINATION "${TREVRPC_RPC_INSTALL_CMAKEDIR}"
    )
    write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_rpcConfigVersion.cmake"
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
    )
    install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_rpcConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_rpcConfigVersion.cmake"
        DESTINATION "${TREVRPC_RPC_INSTALL_CMAKEDIR}"
    )
endif()

if(TREVRPC_BUILD_RPC_MSQUIC)
    install(TARGETS trevrpc_rpc_msquic
        EXPORT trevrpcRpcMsquicTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
    install(EXPORT trevrpcRpcMsquicTargets
        NAMESPACE trevrpc::
        DESTINATION "${TREVRPC_RPC_MSQUIC_INSTALL_CMAKEDIR}"
    )
    configure_package_config_file(
        cmake/trevrpc_rpc_msquicConfig.cmake.in
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_rpc_msquicConfig.cmake"
        INSTALL_DESTINATION "${TREVRPC_RPC_MSQUIC_INSTALL_CMAKEDIR}"
    )
    write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_rpc_msquicConfigVersion.cmake"
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY SameMajorVersion
    )
    install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_rpc_msquicConfig.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_rpc_msquicConfigVersion.cmake"
        DESTINATION "${TREVRPC_RPC_MSQUIC_INSTALL_CMAKEDIR}"
    )
endif()

if(TARGET trevrpc_core)
    if(TREVRPC_BUILD_TRANSPORT_MSQUIC)
        install(FILES $<TARGET_FILE:trevrpc_core>
            DESTINATION ${TREVRPC_TRANSPORT_PRIVATE_INSTALL_LIBDIR}
            RENAME "${CMAKE_STATIC_LIBRARY_PREFIX}trevrpc_transport_private_core${CMAKE_STATIC_LIBRARY_SUFFIX}"
        )
    endif()
    if(TARGET trevrpc_rpc_private_core)
        install(FILES $<TARGET_FILE:trevrpc_rpc_private_core>
            DESTINATION ${TREVRPC_RPC_PRIVATE_INSTALL_LIBDIR}
            RENAME "${CMAKE_STATIC_LIBRARY_PREFIX}trevrpc_rpc_private_core${CMAKE_STATIC_LIBRARY_SUFFIX}"
        )
    endif()
endif()

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
if(TARGET trevrpc_greeter_server)
    install(TARGETS trevrpc_greeter_server
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
endif()
if(trevrpc_pkg_config_files)
    install(FILES ${trevrpc_pkg_config_files}
        DESTINATION "${TREVRPC_INSTALL_PKGCONFIGDIR}"
    )
endif()
if(TREVRPC_BUILD_ENGINE)
    install(FILES
        include/trevrpc_engine.h
        include/trevrpc_engine_provider.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )
endif()
if(TREVRPC_BUILD_ENGINE_MSQUIC)
    install(FILES ${TREVRPC_MSQUIC_INCLUDE_DIR}/trevrpc_engine_msquic.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )
endif()
if(TREVRPC_BUILD_TRANSPORT)
    install(FILES
        include/trevrpc_transport.h
        include/trevrpc_transport_provider.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )
endif()
if(TREVRPC_BUILD_TRANSPORT_MSQUIC)
    install(FILES ${TREVRPC_MSQUIC_INCLUDE_DIR}/trevrpc_transport_msquic.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )
endif()
if(TREVRPC_BUILD_RPC)
    install(FILES include/trevrpc_rpc.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )
endif()
if(TREVRPC_BUILD_RPC_MSQUIC)
    install(FILES ${TREVRPC_MSQUIC_INCLUDE_DIR}/trevrpc_rpc_msquic.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )
endif()
if(TREVRPC_BUILD_TESTS)
    enable_testing()
    enable_language(CXX)

    # Test assertions are part of the test contract even in Release builds.
    # CMake's release flags define NDEBUG before target-local options.
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        add_compile_options(
            $<$<COMPILE_LANGUAGE:C>:-UNDEBUG>
            $<$<COMPILE_LANGUAGE:CXX>:-UNDEBUG>
        )
    elseif(MSVC)
        add_compile_options(/UNDEBUG)
    endif()

    add_library(trevrpc_provider_abi_spike STATIC
        tests/go-provider-spike/provider/archive-src/provider.c
    )
    target_include_directories(trevrpc_provider_abi_spike PRIVATE
        tests/go-provider-spike/provider/include
    )
    trevrpc_c_warnings(trevrpc_provider_abi_spike)
    trevrpc_c_sanitizers(trevrpc_provider_abi_spike)

    add_executable(trevrpc_provider_abi_spike_test
        tests/provider_abi_spike_test.c
        tests/go-provider-spike/neutral/internal/cabi/abi.c
    )
    target_include_directories(trevrpc_provider_abi_spike_test PRIVATE
        tests
        tests/go-provider-spike/neutral/internal/cabi
    )
    target_link_libraries(trevrpc_provider_abi_spike_test PRIVATE trevrpc_provider_abi_spike)
    trevrpc_c_warnings(trevrpc_provider_abi_spike_test)
    trevrpc_c_sanitizers(trevrpc_provider_abi_spike_test)
    add_test(NAME trevrpc_provider_abi_spike COMMAND trevrpc_provider_abi_spike_test)
    add_test(NAME trevrpc_provider_abi_header_sync
        COMMAND ${CMAKE_COMMAND} -E compare_files
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/go-provider-spike/neutral/internal/cabi/abi.h
            ${CMAKE_CURRENT_SOURCE_DIR}/tests/go-provider-spike/provider/include/abi.h
    )

    add_executable(trevrpc_h3_demux_test tests/h3_demux_test.c)
    if(TARGET trevrpc_protocol_core)
        target_link_libraries(trevrpc_h3_demux_test PRIVATE trevrpc_protocol_core)
    else()
        target_sources(trevrpc_h3_demux_test PRIVATE
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_h3_demux.c
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_msquic_features.c
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_quic_varint.c
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_webtransport_profile.c
        )
    endif()
    target_include_directories(trevrpc_h3_demux_test PRIVATE src)
    trevrpc_c_warnings(trevrpc_h3_demux_test)
    trevrpc_c_sanitizers(trevrpc_h3_demux_test)
    add_test(NAME trevrpc_h3_demux COMMAND trevrpc_h3_demux_test)

    add_executable(trevrpc_h3_ingress_test
        tests/h3_ingress_test.c
        ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_h3_ingress.c
    )
    if(TARGET trevrpc_protocol_core)
        target_link_libraries(trevrpc_h3_ingress_test PRIVATE trevrpc_protocol_core)
    else()
        target_sources(trevrpc_h3_ingress_test PRIVATE
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_h3_demux.c
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_msquic_features.c
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_quic_varint.c
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_webtransport_profile.c
        )
    endif()
    target_compile_definitions(trevrpc_h3_ingress_test PRIVATE TREVRPC_H3_INGRESS_TESTING)
    target_include_directories(trevrpc_h3_ingress_test PRIVATE include src)
    target_link_libraries(trevrpc_h3_ingress_test PRIVATE Threads::Threads)
    trevrpc_c_warnings(trevrpc_h3_ingress_test)
    trevrpc_c_sanitizers(trevrpc_h3_ingress_test)
    add_test(NAME trevrpc_h3_ingress COMMAND trevrpc_h3_ingress_test)
    set_tests_properties(trevrpc_h3_ingress PROPERTIES TIMEOUT 30)

    add_executable(trevrpc_http3_frame_test tests/http3_frame_test.c)
    if(TARGET trevrpc_protocol_core)
        target_link_libraries(trevrpc_http3_frame_test PRIVATE trevrpc_protocol_core)
    else()
        target_sources(trevrpc_http3_frame_test PRIVATE
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_http3_frame.c
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_quic_varint.c
        )
    endif()
    target_include_directories(trevrpc_http3_frame_test PRIVATE src)
    trevrpc_c_warnings(trevrpc_http3_frame_test)
    trevrpc_c_sanitizers(trevrpc_http3_frame_test)
    add_test(NAME trevrpc_http3_frame COMMAND trevrpc_http3_frame_test)

    add_executable(trevrpc_http3_headers_test tests/http3_headers_test.c)
    if(TARGET trevrpc_protocol_core)
        target_link_libraries(trevrpc_http3_headers_test PRIVATE trevrpc_protocol_core)
    else()
        target_sources(trevrpc_http3_headers_test PRIVATE ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_http3_headers.c)
    endif()
    target_include_directories(trevrpc_http3_headers_test PRIVATE src)
    trevrpc_c_warnings(trevrpc_http3_headers_test)
    trevrpc_c_sanitizers(trevrpc_http3_headers_test)
    add_test(NAME trevrpc_http3_headers COMMAND trevrpc_http3_headers_test)

    add_executable(trevrpc_http3_settings_test tests/http3_settings_test.c)
    if(TARGET trevrpc_protocol_core)
        target_link_libraries(trevrpc_http3_settings_test PRIVATE trevrpc_protocol_core)
    else()
        target_sources(trevrpc_http3_settings_test PRIVATE
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_http3_settings.c
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_quic_varint.c
        )
    endif()
    target_include_directories(trevrpc_http3_settings_test PRIVATE src)
    trevrpc_c_warnings(trevrpc_http3_settings_test)
    trevrpc_c_sanitizers(trevrpc_http3_settings_test)
    add_test(NAME trevrpc_http3_settings COMMAND trevrpc_http3_settings_test)

    add_executable(trevrpc_qpack_static_test tests/qpack_static_test.c)
    if(TARGET trevrpc_protocol_core)
        target_link_libraries(trevrpc_qpack_static_test PRIVATE trevrpc_protocol_core)
    else()
        target_sources(trevrpc_qpack_static_test PRIVATE ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_qpack_static.c)
    endif()
    target_include_directories(trevrpc_qpack_static_test PRIVATE src)
    trevrpc_c_warnings(trevrpc_qpack_static_test)
    trevrpc_c_sanitizers(trevrpc_qpack_static_test)
    add_test(NAME trevrpc_qpack_static COMMAND trevrpc_qpack_static_test)

    add_executable(trevrpc_quic_varint_test tests/quic_varint_test.c)
    if(TARGET trevrpc_protocol_core)
        target_link_libraries(trevrpc_quic_varint_test PRIVATE trevrpc_protocol_core)
    else()
        target_sources(trevrpc_quic_varint_test PRIVATE ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_quic_varint.c)
    endif()
    target_include_directories(trevrpc_quic_varint_test PRIVATE src)
    trevrpc_c_warnings(trevrpc_quic_varint_test)
    trevrpc_c_sanitizers(trevrpc_quic_varint_test)
    add_test(NAME trevrpc_quic_varint COMMAND trevrpc_quic_varint_test)

    add_executable(trevrpc_msquic_features_test tests/msquic_features_test.c)
    if(TARGET trevrpc_protocol_core)
        target_link_libraries(trevrpc_msquic_features_test PRIVATE trevrpc_protocol_core)
    else()
        target_sources(trevrpc_msquic_features_test PRIVATE ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_msquic_features.c)
    endif()
    target_include_directories(trevrpc_msquic_features_test PRIVATE src)
    trevrpc_c_warnings(trevrpc_msquic_features_test)
    trevrpc_c_sanitizers(trevrpc_msquic_features_test)
    add_test(NAME trevrpc_msquic_features COMMAND trevrpc_msquic_features_test)

    add_executable(trevrpc_webtransport_profile_test tests/webtransport_profile_test.c)
    if(TARGET trevrpc_protocol_core)
        target_link_libraries(trevrpc_webtransport_profile_test PRIVATE trevrpc_protocol_core)
    else()
        target_sources(trevrpc_webtransport_profile_test PRIVATE
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_msquic_features.c
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_webtransport_profile.c
        )
    endif()
    target_include_directories(trevrpc_webtransport_profile_test PRIVATE src)
    trevrpc_c_warnings(trevrpc_webtransport_profile_test)
    trevrpc_c_sanitizers(trevrpc_webtransport_profile_test)
    add_test(NAME trevrpc_webtransport_profile COMMAND trevrpc_webtransport_profile_test)

    add_executable(trevrpc_webtransport_capsule_test tests/webtransport_capsule_test.c)
    if(TARGET trevrpc_protocol_core)
        target_link_libraries(trevrpc_webtransport_capsule_test PRIVATE trevrpc_protocol_core)
    else()
        target_sources(trevrpc_webtransport_capsule_test PRIVATE
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_webtransport_capsule.c
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_quic_varint.c
        )
    endif()
    target_include_directories(trevrpc_webtransport_capsule_test PRIVATE src)
    trevrpc_c_warnings(trevrpc_webtransport_capsule_test)
    trevrpc_c_sanitizers(trevrpc_webtransport_capsule_test)
    add_test(NAME trevrpc_webtransport_capsule COMMAND trevrpc_webtransport_capsule_test)

    foreach(trevrpc_provider_internal_test IN ITEMS
        trevrpc_h3_demux_test
        trevrpc_h3_ingress_test
        trevrpc_http3_frame_test
        trevrpc_http3_headers_test
        trevrpc_http3_settings_test
        trevrpc_qpack_static_test
        trevrpc_quic_varint_test
        trevrpc_msquic_features_test
        trevrpc_webtransport_profile_test
        trevrpc_webtransport_capsule_test
    )
        target_include_directories(${trevrpc_provider_internal_test} PRIVATE
            ${TREVRPC_MSQUIC_INCLUDE_DIR}
            ${TREVRPC_MSQUIC_SOURCE_DIR}
        )
    endforeach()

    if(TARGET trevrpc_msquic_native_core)
        add_test(NAME trevrpc_removed_abi6_surface
            COMMAND "${CMAKE_COMMAND}"
                -DTREVRPC_NM=${CMAKE_NM}
                -DTREVRPC_AR=${CMAKE_AR}
                "-DTREVRPC_ARCHIVES=$<TARGET_FILE:trevrpc_msquic_native_core>"
                -DTREVRPC_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR}
                -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/abi/check_removed_abi6.cmake"
        )
    endif()

    if(TREVRPC_BUILD_ENGINE)
        add_executable(trevrpc_engine_api_test tests/engine-abi1/public_api.c)
        target_link_libraries(trevrpc_engine_api_test PRIVATE trevrpc_engine)
        trevrpc_c_warnings(trevrpc_engine_api_test)
        trevrpc_c_sanitizers(trevrpc_engine_api_test)
        add_test(NAME trevrpc_engine_api COMMAND trevrpc_engine_api_test)

        add_executable(trevrpc_engine_api_cpp_test tests/engine-abi1/public_api.cpp)
        target_link_libraries(trevrpc_engine_api_cpp_test PRIVATE trevrpc_engine)
        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(trevrpc_engine_api_cpp_test PRIVATE -Wall -Wextra -Werror)
        endif()
        trevrpc_c_sanitizers(trevrpc_engine_api_cpp_test)
        add_test(NAME trevrpc_engine_api_cpp COMMAND trevrpc_engine_api_cpp_test)

        add_executable(trevrpc_engine_provider_abi_test tests/engine_provider_abi_test.c)
        target_link_libraries(trevrpc_engine_provider_abi_test PRIVATE trevrpc_engine)
        trevrpc_c_warnings(trevrpc_engine_provider_abi_test)
        trevrpc_c_sanitizers(trevrpc_engine_provider_abi_test)
        add_test(NAME trevrpc_engine_provider_abi COMMAND trevrpc_engine_provider_abi_test)

        add_test(NAME trevrpc_engine_abi1_symbols
            COMMAND "${CMAKE_COMMAND}"
                -DTREVRPC_NM=${CMAKE_NM}
                "-DTREVRPC_ARCHIVES=$<TARGET_FILE:trevrpc_engine>"
                "-DTREVRPC_PUBLIC_HEADERS=${CMAKE_CURRENT_SOURCE_DIR}/include/trevrpc_engine.h;${CMAKE_CURRENT_SOURCE_DIR}/include/trevrpc_engine_provider.h"
                -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/engine-abi1/check_symbols.cmake"
        )

        add_executable(trevrpc_engine_queue_test tests/engine_queue_test.c)
        target_include_directories(trevrpc_engine_queue_test PRIVATE src)
        target_link_libraries(trevrpc_engine_queue_test PRIVATE trevrpc_engine_testing)
        trevrpc_c_warnings(trevrpc_engine_queue_test)
        trevrpc_c_sanitizers(trevrpc_engine_queue_test)
        add_test(NAME trevrpc_engine_queue COMMAND trevrpc_engine_queue_test)

        add_executable(trevrpc_engine_wake_test tests/engine_wake_test.c)
        target_include_directories(trevrpc_engine_wake_test PRIVATE src)
        target_link_libraries(trevrpc_engine_wake_test PRIVATE trevrpc_engine_testing)
        trevrpc_c_warnings(trevrpc_engine_wake_test)
        trevrpc_c_sanitizers(trevrpc_engine_wake_test)
        add_test(NAME trevrpc_engine_wake COMMAND trevrpc_engine_wake_test)
        set_tests_properties(trevrpc_engine_wake PROPERTIES TIMEOUT 30)

        add_executable(trevrpc_engine_lifecycle_test tests/engine_lifecycle_test.c)
        target_include_directories(trevrpc_engine_lifecycle_test PRIVATE src)
        target_link_libraries(trevrpc_engine_lifecycle_test PRIVATE trevrpc_engine_testing)
        trevrpc_c_warnings(trevrpc_engine_lifecycle_test)
        trevrpc_c_sanitizers(trevrpc_engine_lifecycle_test)
        add_test(NAME trevrpc_engine_lifecycle COMMAND trevrpc_engine_lifecycle_test)
        set_tests_properties(trevrpc_engine_lifecycle PROPERTIES TIMEOUT 60)
        trevrpc_c_tsan_suppressions(trevrpc_engine_lifecycle)

        add_executable(trevrpc_engine_dispatch_test
            tests/engine_dispatch_test.c
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_rpc_transport_engine.c
        )
        target_include_directories(trevrpc_engine_dispatch_test PRIVATE
            src
            ${TREVRPC_MSQUIC_INCLUDE_DIR}
            ${TREVRPC_MSQUIC_SOURCE_DIR}
        )
        target_compile_definitions(trevrpc_engine_dispatch_test PRIVATE TREVRPC_CREDENTIAL_TESTING)
        target_link_libraries(trevrpc_engine_dispatch_test PRIVATE trevrpc_engine trevrpc_credential_testing)
        trevrpc_c_warnings(trevrpc_engine_dispatch_test)
        trevrpc_c_sanitizers(trevrpc_engine_dispatch_test)
        add_test(NAME trevrpc_engine_dispatch COMMAND trevrpc_engine_dispatch_test)
    endif()

    if(TREVRPC_BUILD_ENGINE_MSQUIC)
        add_executable(trevrpc_engine_msquic_api_test tests/engine-msquic-abi1/public_api.c)
        target_link_libraries(trevrpc_engine_msquic_api_test PRIVATE trevrpc_engine_msquic)
        trevrpc_c_warnings(trevrpc_engine_msquic_api_test)
        trevrpc_c_sanitizers(trevrpc_engine_msquic_api_test)
        add_test(NAME trevrpc_engine_msquic_api COMMAND trevrpc_engine_msquic_api_test)

        add_executable(trevrpc_engine_msquic_api_cpp_test tests/engine-msquic-abi1/public_api.cpp)
        target_link_libraries(trevrpc_engine_msquic_api_cpp_test PRIVATE trevrpc_engine_msquic)
        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(trevrpc_engine_msquic_api_cpp_test PRIVATE -Wall -Wextra -Werror)
        endif()
        trevrpc_c_sanitizers(trevrpc_engine_msquic_api_cpp_test)
        add_test(NAME trevrpc_engine_msquic_api_cpp COMMAND trevrpc_engine_msquic_api_cpp_test)

        add_test(NAME trevrpc_engine_msquic_abi1_symbols
            COMMAND "${CMAKE_COMMAND}"
                -DTREVRPC_NM=${CMAKE_NM}
                "-DTREVRPC_ARCHIVES=$<TARGET_FILE:trevrpc_engine_msquic>"
                "-DTREVRPC_PUBLIC_HEADERS=${TREVRPC_MSQUIC_INCLUDE_DIR}/trevrpc_engine_msquic.h"
                -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/engine-msquic-abi1/check_symbols.cmake"
        )

        add_executable(trevrpc_engine_msquic_smoke_test tests/engine_msquic_smoke_test.c)
        add_dependencies(trevrpc_engine_msquic_smoke_test trevrpc_msquic_test_certificates)
        target_compile_definitions(trevrpc_engine_msquic_smoke_test PRIVATE
            TREVRPC_MSQUIC_TEST_CERT="${trevrpc_msquic_test_cert}"
            TREVRPC_MSQUIC_TEST_KEY="${trevrpc_msquic_test_key}"
        )
        target_link_libraries(trevrpc_engine_msquic_smoke_test PRIVATE trevrpc_engine_msquic)
        trevrpc_c_warnings(trevrpc_engine_msquic_smoke_test)
        trevrpc_c_sanitizers(trevrpc_engine_msquic_smoke_test)
        add_test(NAME trevrpc_engine_msquic_smoke COMMAND trevrpc_engine_msquic_smoke_test)
        set_tests_properties(trevrpc_engine_msquic_smoke PROPERTIES TIMEOUT 30)
        trevrpc_c_msquic_loopback_test(trevrpc_engine_msquic_smoke)
        trevrpc_c_lsan_suppressions(trevrpc_engine_msquic_smoke)
        trevrpc_c_tsan_suppressions(trevrpc_engine_msquic_smoke)

        add_executable(trevrpc_engine_msquic_backpressure_test tests/engine_msquic_backpressure_test.c)
        target_compile_definitions(trevrpc_engine_msquic_backpressure_test PRIVATE QUIC_API_ENABLE_PREVIEW_FEATURES)
        target_include_directories(trevrpc_engine_msquic_backpressure_test PRIVATE
            include
            src
            ${TREVRPC_MSQUIC_INCLUDE_DIR}
            ${TREVRPC_MSQUIC_SOURCE_DIR}
        )
        target_link_libraries(trevrpc_engine_msquic_backpressure_test PRIVATE
            trevrpc_engine
            trevrpc_engine_msquic_support
            Threads::Threads
            msquic::msquic
        )
        trevrpc_c_warnings(trevrpc_engine_msquic_backpressure_test)
        trevrpc_c_sanitizers(trevrpc_engine_msquic_backpressure_test)
        add_test(NAME trevrpc_engine_msquic_backpressure COMMAND trevrpc_engine_msquic_backpressure_test)
        set_tests_properties(trevrpc_engine_msquic_backpressure PROPERTIES TIMEOUT 30)
        trevrpc_c_lsan_suppressions(trevrpc_engine_msquic_backpressure)
        trevrpc_c_tsan_suppressions(trevrpc_engine_msquic_backpressure)
    endif()

    if(TARGET trevrpc_transport)
        add_executable(trevrpc_transport_api_test tests/transport_public_api_test.c)
        target_link_libraries(trevrpc_transport_api_test PRIVATE trevrpc_transport)
        trevrpc_c_warnings(trevrpc_transport_api_test)
        trevrpc_c_sanitizers(trevrpc_transport_api_test)
        add_test(NAME trevrpc_transport_api COMMAND trevrpc_transport_api_test)

        add_executable(trevrpc_transport_api_cpp_test tests/transport_public_api_test.cpp)
        target_link_libraries(trevrpc_transport_api_cpp_test PRIVATE trevrpc_transport)
        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(trevrpc_transport_api_cpp_test PRIVATE -Wall -Wextra -Werror)
        endif()
        trevrpc_c_sanitizers(trevrpc_transport_api_cpp_test)
        add_test(NAME trevrpc_transport_api_cpp COMMAND trevrpc_transport_api_cpp_test)

        add_executable(trevrpc_transport_provider_abi_test
            tests/transport_provider_abi_test.c
            internal/cabi/testprovider/provider.c
        )
        target_include_directories(trevrpc_transport_provider_abi_test PRIVATE internal/cabi/testprovider)
        target_link_libraries(trevrpc_transport_provider_abi_test PRIVATE trevrpc_transport)
        trevrpc_c_warnings(trevrpc_transport_provider_abi_test)
        trevrpc_c_sanitizers(trevrpc_transport_provider_abi_test)
        add_test(NAME trevrpc_transport_provider_abi COMMAND trevrpc_transport_provider_abi_test)

        add_test(NAME trevrpc_transport_abi1_symbols
            COMMAND "${CMAKE_COMMAND}"
                -DTREVRPC_NM=${CMAKE_NM}
                "-DTREVRPC_ARCHIVES=$<TARGET_FILE:trevrpc_transport>"
                "-DTREVRPC_PUBLIC_HEADERS=${CMAKE_CURRENT_SOURCE_DIR}/include/trevrpc_transport.h;${CMAKE_CURRENT_SOURCE_DIR}/include/trevrpc_transport_provider.h"
                -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/transport-abi1/check_symbols.cmake"
        )

        if(TARGET trevrpc_transport_testing)
            add_executable(trevrpc_transport_testing_test tests/transport_testing_test.c)
            target_link_libraries(trevrpc_transport_testing_test PRIVATE trevrpc_transport_testing)
            trevrpc_c_warnings(trevrpc_transport_testing_test)
            trevrpc_c_sanitizers(trevrpc_transport_testing_test)
            add_test(NAME trevrpc_transport_testing COMMAND trevrpc_transport_testing_test)
            set_tests_properties(trevrpc_transport_testing PROPERTIES TIMEOUT 30)
        endif()
    endif()

    if(TARGET trevrpc_transport_msquic)
        add_test(NAME trevrpc_transport_msquic_abi1_symbols
            COMMAND "${CMAKE_COMMAND}"
                -DTREVRPC_NM=${CMAKE_NM}
                "-DTREVRPC_ARCHIVES=$<TARGET_FILE:trevrpc_transport_msquic>"
                "-DTREVRPC_PUBLIC_HEADERS=${TREVRPC_MSQUIC_INCLUDE_DIR}/trevrpc_transport_msquic.h"
                -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/transport-msquic-abi1/check_symbols.cmake"
        )

        add_executable(trevrpc_transport_msquic_smoke_test tests/transport_msquic_smoke_test.c)
        target_link_libraries(trevrpc_transport_msquic_smoke_test PRIVATE trevrpc_transport_msquic)
        trevrpc_c_warnings(trevrpc_transport_msquic_smoke_test)
        trevrpc_c_sanitizers(trevrpc_transport_msquic_smoke_test)
        add_test(NAME trevrpc_transport_msquic_smoke COMMAND trevrpc_transport_msquic_smoke_test)
        set_tests_properties(trevrpc_transport_msquic_smoke PROPERTIES TIMEOUT 30)
        trevrpc_c_lsan_suppressions(trevrpc_transport_msquic_smoke)
        trevrpc_c_tsan_suppressions(trevrpc_transport_msquic_smoke)

        add_executable(trevrpc_transport_native_roundtrip_test tests/transport_native_roundtrip_test.c)
        add_dependencies(trevrpc_transport_native_roundtrip_test trevrpc_msquic_test_certificates)
        target_compile_definitions(trevrpc_transport_native_roundtrip_test PRIVATE
            TREVRPC_CREDENTIAL_TESTING
            TREVRPC_MSQUIC_TEST_CA_CERT="${trevrpc_msquic_test_ca_cert}"
            TREVRPC_MSQUIC_TEST_CERT="${trevrpc_msquic_test_cert}"
            TREVRPC_MSQUIC_TEST_KEY="${trevrpc_msquic_test_key}"
        )
        target_link_libraries(trevrpc_transport_native_roundtrip_test PRIVATE trevrpc_transport_msquic_testing)
        trevrpc_c_warnings(trevrpc_transport_native_roundtrip_test)
        trevrpc_c_sanitizers(trevrpc_transport_native_roundtrip_test)
        add_test(NAME trevrpc_transport_native_roundtrip COMMAND trevrpc_transport_native_roundtrip_test)
        set_tests_properties(trevrpc_transport_native_roundtrip PROPERTIES TIMEOUT 30)
        trevrpc_c_msquic_loopback_test(trevrpc_transport_native_roundtrip)
        trevrpc_c_lsan_suppressions(trevrpc_transport_native_roundtrip)
        trevrpc_c_tsan_suppressions(trevrpc_transport_native_roundtrip)

        add_executable(trevrpc_transport_h3_memory_roundtrip_test tests/transport_native_roundtrip_test.c)
        add_dependencies(trevrpc_transport_h3_memory_roundtrip_test trevrpc_msquic_test_certificates)
        target_compile_definitions(trevrpc_transport_h3_memory_roundtrip_test PRIVATE
            TREVRPC_CREDENTIAL_TESTING
            TREVRPC_MSQUIC_TEST_CA_CERT="${trevrpc_msquic_test_ca_cert}"
            TREVRPC_MSQUIC_TEST_CERT="${trevrpc_msquic_test_cert}"
            TREVRPC_MSQUIC_TEST_KEY="${trevrpc_msquic_test_key}"
            TREVRPC_TRANSPORT_TEST_PROTOCOL=TREVRPC_TRANSPORT_PROTOCOL_HTTP3
        )
        target_link_libraries(trevrpc_transport_h3_memory_roundtrip_test PRIVATE trevrpc_transport_msquic_testing)
        trevrpc_c_warnings(trevrpc_transport_h3_memory_roundtrip_test)
        trevrpc_c_sanitizers(trevrpc_transport_h3_memory_roundtrip_test)
        add_test(NAME trevrpc_transport_h3_memory_roundtrip COMMAND trevrpc_transport_h3_memory_roundtrip_test)
        set_tests_properties(trevrpc_transport_h3_memory_roundtrip PROPERTIES TIMEOUT 30)
        trevrpc_c_msquic_loopback_test(trevrpc_transport_h3_memory_roundtrip)
        trevrpc_c_lsan_suppressions(trevrpc_transport_h3_memory_roundtrip)
        trevrpc_c_tsan_suppressions(trevrpc_transport_h3_memory_roundtrip)
    endif()

    if(TARGET trevrpc_rpc)
        add_executable(trevrpc_rpc_api_test tests/rpc-abi1/public_api.c)
        if(TARGET trevrpc_rpc_msquic)
            target_compile_definitions(trevrpc_rpc_api_test PRIVATE TREVRPC_RPC_TEST_MSQUIC)
            target_link_libraries(trevrpc_rpc_api_test PRIVATE trevrpc_rpc_msquic)
        else()
            target_link_libraries(trevrpc_rpc_api_test PRIVATE trevrpc_rpc)
        endif()
        trevrpc_c_warnings(trevrpc_rpc_api_test)
        trevrpc_c_sanitizers(trevrpc_rpc_api_test)
        add_test(NAME trevrpc_rpc_api COMMAND trevrpc_rpc_api_test)
        set_tests_properties(trevrpc_rpc_api PROPERTIES TIMEOUT 30)

        add_executable(trevrpc_rpc_api_cpp_test tests/rpc-abi1/public_api.cpp)
        if(TARGET trevrpc_rpc_msquic)
            target_compile_definitions(trevrpc_rpc_api_cpp_test PRIVATE TREVRPC_RPC_TEST_MSQUIC)
            target_link_libraries(trevrpc_rpc_api_cpp_test PRIVATE trevrpc_rpc_msquic)
        else()
            target_link_libraries(trevrpc_rpc_api_cpp_test PRIVATE trevrpc_rpc)
        endif()
        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
            target_compile_options(trevrpc_rpc_api_cpp_test PRIVATE -Wall -Wextra -Werror)
        endif()
        trevrpc_c_sanitizers(trevrpc_rpc_api_cpp_test)
        add_test(NAME trevrpc_rpc_api_cpp COMMAND trevrpc_rpc_api_cpp_test)
        set_tests_properties(trevrpc_rpc_api_cpp PROPERTIES TIMEOUT 30)

        add_executable(trevrpc_rpc_fake_transport_test tests/rpc_fake_transport_test.c)
        target_include_directories(trevrpc_rpc_fake_transport_test PRIVATE
            src
            ${TREVRPC_MSQUIC_INCLUDE_DIR}
            ${TREVRPC_MSQUIC_SOURCE_DIR}
        )
        target_link_libraries(trevrpc_rpc_fake_transport_test PRIVATE trevrpc_rpc_fake_transport_support)
        trevrpc_c_warnings(trevrpc_rpc_fake_transport_test)
        trevrpc_c_sanitizers(trevrpc_rpc_fake_transport_test)
        add_test(NAME trevrpc_rpc_fake_transport COMMAND trevrpc_rpc_fake_transport_test)
        set_tests_properties(trevrpc_rpc_fake_transport PROPERTIES TIMEOUT 30)

        add_executable(trevrpc_rpc_registry_test tests/rpc_registry_test.c)
        target_include_directories(trevrpc_rpc_registry_test PRIVATE
            src
            tests
            ${TREVRPC_MSQUIC_SOURCE_DIR}
        )
        target_link_libraries(trevrpc_rpc_registry_test PRIVATE trevrpc_rpc_fake_transport_support)
        trevrpc_c_warnings(trevrpc_rpc_registry_test)
        trevrpc_c_sanitizers(trevrpc_rpc_registry_test)
        add_test(NAME trevrpc_rpc_registry COMMAND trevrpc_rpc_registry_test)
        set_tests_properties(trevrpc_rpc_registry PROPERTIES TIMEOUT 30)

        add_test(NAME trevrpc_rpc_abi1_symbols
            COMMAND "${CMAKE_COMMAND}"
                -DTREVRPC_NM=${CMAKE_NM}
                -DTREVRPC_ARCHIVES=$<TARGET_FILE:trevrpc_rpc>
                -DTREVRPC_PUBLIC_HEADERS=${CMAKE_CURRENT_SOURCE_DIR}/include/trevrpc_rpc.h
                -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/rpc-abi1/check_symbols.cmake"
        )
        set_tests_properties(trevrpc_rpc_abi1_symbols PROPERTIES TIMEOUT 30)

    endif()

    if(TARGET trevrpc_rpc_msquic)
        add_test(NAME trevrpc_rpc_msquic_abi1_symbols
            COMMAND "${CMAKE_COMMAND}"
                -DTREVRPC_NM=${CMAKE_NM}
                -DTREVRPC_ARCHIVES=$<TARGET_FILE:trevrpc_rpc_msquic>
                -DTREVRPC_PUBLIC_HEADERS=${TREVRPC_MSQUIC_INCLUDE_DIR}/trevrpc_rpc_msquic.h
                -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/rpc-abi1/check_msquic_symbols.cmake"
        )
        set_tests_properties(trevrpc_rpc_msquic_abi1_symbols PROPERTIES TIMEOUT 30)

        add_executable(trevrpc_rpc_msquic_smoke_test tests/rpc_msquic_smoke_test.c)
        target_link_libraries(trevrpc_rpc_msquic_smoke_test PRIVATE trevrpc_rpc_msquic)
        trevrpc_c_warnings(trevrpc_rpc_msquic_smoke_test)
        trevrpc_c_sanitizers(trevrpc_rpc_msquic_smoke_test)
        add_test(NAME trevrpc_rpc_msquic_smoke COMMAND trevrpc_rpc_msquic_smoke_test)
        set_tests_properties(trevrpc_rpc_msquic_smoke PROPERTIES TIMEOUT 30)
        trevrpc_c_lsan_suppressions(trevrpc_rpc_msquic_smoke)
        trevrpc_c_tsan_suppressions(trevrpc_rpc_msquic_smoke)

        add_executable(trevrpc_rpc_transport_h3_test
            tests/rpc_transport_h3_test.c
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_rpc_transport_h3.c
            ${TREVRPC_MSQUIC_SOURCE_DIR}/trevrpc_rpc_transport_msquic.c
        )
        target_compile_definitions(trevrpc_rpc_transport_h3_test PRIVATE
            TREVRPC_MSQUIC_TESTING
            TREVRPC_RPC_TRANSPORT_H3_TESTING
        )
        target_include_directories(trevrpc_rpc_transport_h3_test PRIVATE
            include
            src
            ${TREVRPC_MSQUIC_INCLUDE_DIR}
            ${TREVRPC_MSQUIC_SOURCE_DIR}
        )
        target_link_libraries(trevrpc_rpc_transport_h3_test PRIVATE
            trevrpc_msquic_testing
            trevrpc_protocol_core
            msquic::msquic
        )
        trevrpc_c_warnings(trevrpc_rpc_transport_h3_test)
        trevrpc_c_sanitizers(trevrpc_rpc_transport_h3_test)
        add_test(NAME trevrpc_rpc_transport_h3 COMMAND trevrpc_rpc_transport_h3_test)
        set_tests_properties(trevrpc_rpc_transport_h3 PROPERTIES TIMEOUT 30)
        trevrpc_c_lsan_suppressions(trevrpc_rpc_transport_h3)
        trevrpc_c_tsan_suppressions(trevrpc_rpc_transport_h3)

        add_executable(trevrpc_rpc_transport_msquic_lifecycle_test
            tests/rpc_transport_msquic_lifecycle_test.c)
        target_include_directories(trevrpc_rpc_transport_msquic_lifecycle_test PRIVATE
            include
            src
            tests
            ${TREVRPC_MSQUIC_INCLUDE_DIR}
            ${TREVRPC_MSQUIC_SOURCE_DIR}
        )
        target_link_libraries(trevrpc_rpc_transport_msquic_lifecycle_test PRIVATE
            trevrpc_rpc_fake_transport_support
            trevrpc_rpc_msquic)
        trevrpc_c_warnings(trevrpc_rpc_transport_msquic_lifecycle_test)
        trevrpc_c_sanitizers(trevrpc_rpc_transport_msquic_lifecycle_test)
        add_test(NAME trevrpc_rpc_transport_msquic_lifecycle COMMAND trevrpc_rpc_transport_msquic_lifecycle_test)
        set_tests_properties(trevrpc_rpc_transport_msquic_lifecycle PROPERTIES TIMEOUT 30)

        add_executable(trevrpc_rpc_native_roundtrip_test tests/rpc_native_roundtrip_test.c)
        add_dependencies(trevrpc_rpc_native_roundtrip_test trevrpc_msquic_test_certificates)
        target_compile_definitions(trevrpc_rpc_native_roundtrip_test PRIVATE
            TREVRPC_MSQUIC_TEST_CA_CERT="${trevrpc_msquic_test_ca_cert}"
            TREVRPC_MSQUIC_TEST_CERT="${trevrpc_msquic_test_cert}"
            TREVRPC_MSQUIC_TEST_KEY="${trevrpc_msquic_test_key}"
        )
        target_link_libraries(trevrpc_rpc_native_roundtrip_test PRIVATE trevrpc_rpc_msquic_testing)
        trevrpc_c_warnings(trevrpc_rpc_native_roundtrip_test)
        trevrpc_c_sanitizers(trevrpc_rpc_native_roundtrip_test)
        add_test(NAME trevrpc_rpc_native_roundtrip COMMAND trevrpc_rpc_native_roundtrip_test)
        set_tests_properties(trevrpc_rpc_native_roundtrip PROPERTIES TIMEOUT 30)
        trevrpc_c_msquic_loopback_test(trevrpc_rpc_native_roundtrip)
        trevrpc_c_lsan_suppressions(trevrpc_rpc_native_roundtrip)
        trevrpc_c_tsan_suppressions(trevrpc_rpc_native_roundtrip)

        add_executable(trevrpc_rpc_native_shapes_test tests/rpc_native_shapes_test.c)
        add_dependencies(trevrpc_rpc_native_shapes_test trevrpc_msquic_test_certificates)
        target_compile_definitions(trevrpc_rpc_native_shapes_test PRIVATE
            TREVRPC_MSQUIC_TEST_CERT="${trevrpc_msquic_test_cert}"
            TREVRPC_MSQUIC_TEST_KEY="${trevrpc_msquic_test_key}"
        )
        target_link_libraries(trevrpc_rpc_native_shapes_test PRIVATE trevrpc_rpc_msquic_testing)
        trevrpc_c_warnings(trevrpc_rpc_native_shapes_test)
        trevrpc_c_sanitizers(trevrpc_rpc_native_shapes_test)
        add_test(NAME trevrpc_rpc_native_shapes COMMAND trevrpc_rpc_native_shapes_test)
        set_tests_properties(trevrpc_rpc_native_shapes PROPERTIES TIMEOUT 30)
        trevrpc_c_msquic_loopback_test(trevrpc_rpc_native_shapes)
        trevrpc_c_lsan_suppressions(trevrpc_rpc_native_shapes)
        trevrpc_c_tsan_suppressions(trevrpc_rpc_native_shapes)

        add_executable(trevrpc_rpc_h3_shapes_test tests/rpc_native_shapes_test.c)
        add_dependencies(trevrpc_rpc_h3_shapes_test trevrpc_msquic_test_certificates)
        target_compile_definitions(trevrpc_rpc_h3_shapes_test PRIVATE
            TREVRPC_MSQUIC_TEST_CERT="${trevrpc_msquic_test_cert}"
            TREVRPC_MSQUIC_TEST_KEY="${trevrpc_msquic_test_key}"
            TREVRPC_RPC_TEST_TRANSPORT=TREVRPC_RPC_MSQUIC_TRANSPORT_HTTP3
        )
        target_link_libraries(trevrpc_rpc_h3_shapes_test PRIVATE trevrpc_rpc_msquic_testing)
        trevrpc_c_warnings(trevrpc_rpc_h3_shapes_test)
        trevrpc_c_sanitizers(trevrpc_rpc_h3_shapes_test)
        add_test(NAME trevrpc_rpc_h3_shapes COMMAND trevrpc_rpc_h3_shapes_test)
        set_tests_properties(trevrpc_rpc_h3_shapes PROPERTIES TIMEOUT 30)
        trevrpc_c_msquic_loopback_test(trevrpc_rpc_h3_shapes)
        trevrpc_c_lsan_suppressions(trevrpc_rpc_h3_shapes)
        trevrpc_c_tsan_suppressions(trevrpc_rpc_h3_shapes)

        add_executable(trevrpc_rpc_wt_shapes_test tests/rpc_native_shapes_test.c)
        add_dependencies(trevrpc_rpc_wt_shapes_test trevrpc_msquic_test_certificates)
        target_compile_definitions(trevrpc_rpc_wt_shapes_test PRIVATE
            TREVRPC_MSQUIC_TEST_CERT="${trevrpc_msquic_test_cert}"
            TREVRPC_MSQUIC_TEST_KEY="${trevrpc_msquic_test_key}"
            TREVRPC_RPC_TEST_TRANSPORT=TREVRPC_RPC_MSQUIC_TRANSPORT_WEBTRANSPORT
        )
        target_link_libraries(trevrpc_rpc_wt_shapes_test PRIVATE trevrpc_rpc_msquic_testing)
        trevrpc_c_warnings(trevrpc_rpc_wt_shapes_test)
        trevrpc_c_sanitizers(trevrpc_rpc_wt_shapes_test)
        add_test(NAME trevrpc_rpc_wt_shapes COMMAND trevrpc_rpc_wt_shapes_test)
        set_tests_properties(trevrpc_rpc_wt_shapes PROPERTIES TIMEOUT 30)
        trevrpc_c_msquic_loopback_test(trevrpc_rpc_wt_shapes)
        trevrpc_c_lsan_suppressions(trevrpc_rpc_wt_shapes)
        trevrpc_c_tsan_suppressions(trevrpc_rpc_wt_shapes)
        foreach(profile IN ITEMS "draft02;1" "draft07;2" "draft14;4" "draft15;8")
            list(GET profile 0 profile_name)
            list(GET profile 1 profile_mask)
            set(test_name "trevrpc_rpc_wt_shapes_${profile_name}")
            add_test(NAME "${test_name}" COMMAND trevrpc_rpc_wt_shapes_test "${profile_mask}")
            set_tests_properties("${test_name}" PROPERTIES TIMEOUT 30)
            trevrpc_c_msquic_loopback_test("${test_name}")
            trevrpc_c_lsan_suppressions("${test_name}")
            trevrpc_c_tsan_suppressions("${test_name}")
        endforeach()
    endif()

    if(TARGET trevrpc_core)
        add_executable(trevrpc_frame_test tests/frame_test.c)
        target_include_directories(
            trevrpc_frame_test PRIVATE src ${TREVRPC_MSQUIC_SOURCE_DIR})
        target_link_libraries(trevrpc_frame_test PRIVATE trevrpc_core)
        trevrpc_c_warnings(trevrpc_frame_test)
        trevrpc_c_sanitizers(trevrpc_frame_test)

        add_test(NAME trevrpc_frame COMMAND trevrpc_frame_test)
    endif()

    if(TARGET trevrpc_greeter_server AND TARGET trevrpc_greeter_client)
        foreach(transport IN ITEMS native http3 webtransport)
            set(test_name "trevrpc_greeter_${transport}_smoke")
            add_test(NAME "${test_name}"
                COMMAND sh "${CMAKE_CURRENT_SOURCE_DIR}/tests/greeter_rpc_smoke.sh"
                    $<TARGET_FILE:trevrpc_greeter_server>
                    $<TARGET_FILE:trevrpc_greeter_client>
                    "${trevrpc_msquic_test_cert}"
                    "${trevrpc_msquic_test_key}"
                    "${transport}"
            )
            set_tests_properties("${test_name}" PROPERTIES TIMEOUT 30)
            trevrpc_c_msquic_loopback_test("${test_name}")
            trevrpc_c_lsan_suppressions("${test_name}")
            trevrpc_c_tsan_suppressions("${test_name}")
        endforeach()
    endif()

    if(TARGET trevrpc_bench_peer_c)
        add_test(NAME trevrpc_bench_peer_capabilities COMMAND trevrpc_bench_peer_c capabilities)
        set_tests_properties(trevrpc_bench_peer_capabilities PROPERTIES
            PASS_REGULAR_EXPRESSION
                "\"schema_version\":5.*\"roles\":\\{\"client\":\\[\"trevrpc_native_quic\",\"trevrpc_webtransport\"\\],\"server\":\\[\"trevrpc_native_quic\",\"trevrpc_http3\",\"trevrpc_webtransport\"\\]\\}.*\"rpc_kinds\":\\[\"unary\",\"client_stream\",\"server_stream\",\"bidi\"\\].*\"histogram\":\"log_linear_v1\""
        )
        add_test(NAME trevrpc_bench_peer_native_smoke
            COMMAND sh "${CMAKE_CURRENT_SOURCE_DIR}/tests/benchmark_peer_native_smoke.sh"
                $<TARGET_FILE:trevrpc_bench_peer_c>
                "${trevrpc_msquic_test_cert}"
                "${trevrpc_msquic_test_key}"
                "${trevrpc_msquic_test_ca_cert}"
        )
        set_tests_properties(trevrpc_bench_peer_native_smoke PROPERTIES TIMEOUT 90)
        trevrpc_c_msquic_loopback_test(trevrpc_bench_peer_native_smoke)
        trevrpc_c_lsan_suppressions(trevrpc_bench_peer_native_smoke)
        trevrpc_c_tsan_suppressions(trevrpc_bench_peer_native_smoke)
        add_test(NAME trevrpc_bench_peer_webtransport_config
            COMMAND "${CMAKE_COMMAND}"
                -DPEER=$<TARGET_FILE:trevrpc_bench_peer_c>
                -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/benchmark_peer_webtransport_config.cmake"
        )
        add_test(NAME trevrpc_bench_peer_http3_lifecycle
            COMMAND sh
                "${CMAKE_CURRENT_SOURCE_DIR}/../bench/http3-lifecycle-test.sh"
                "$<TARGET_FILE:trevrpc_bench_peer_c>"
                "${trevrpc_msquic_test_cert}"
                "${trevrpc_msquic_test_key}"
        )
        set_tests_properties(trevrpc_bench_peer_http3_lifecycle PROPERTIES TIMEOUT 30)
        trevrpc_c_lsan_suppressions(trevrpc_bench_peer_http3_lifecycle)
        add_test(NAME trevrpc_bench_peer_webtransport_smoke
            COMMAND sh "${CMAKE_CURRENT_SOURCE_DIR}/tests/benchmark_peer_webtransport_smoke.sh"
                $<TARGET_FILE:trevrpc_bench_peer_c>
                "${trevrpc_msquic_test_cert}"
                "${trevrpc_msquic_test_key}"
                "${trevrpc_msquic_test_ca_cert}"
        )
        set_tests_properties(trevrpc_bench_peer_webtransport_smoke PROPERTIES TIMEOUT 90)
        trevrpc_c_msquic_loopback_test(trevrpc_bench_peer_webtransport_smoke)
        trevrpc_c_lsan_suppressions(trevrpc_bench_peer_webtransport_smoke)
        trevrpc_c_tsan_suppressions(trevrpc_bench_peer_webtransport_smoke)
    endif()

    if(TARGET trevrpc_rpc_msquic AND trevrpc_needs_generated_test_bindings)
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
        target_include_directories(trevrpc_generated_service_test PRIVATE "${trevrpc_generated_test_dir}")
        target_link_libraries(trevrpc_generated_service_test PRIVATE trevrpc_rpc_msquic protobuf-c)
        trevrpc_c_warnings(trevrpc_generated_service_test)
        trevrpc_c_sanitizers(trevrpc_generated_service_test)

        foreach(transport IN ITEMS native http3 webtransport)
            set(test_name "trevrpc_generated_service_${transport}")
            add_test(NAME "${test_name}" COMMAND trevrpc_generated_service_test "${transport}")
            set_tests_properties("${test_name}" PROPERTIES TIMEOUT 30)
            trevrpc_c_msquic_loopback_test("${test_name}")
            trevrpc_c_lsan_suppressions("${test_name}")
            trevrpc_c_tsan_suppressions("${test_name}")
        endforeach()
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

    if(trevrpc_needs_generated_bindings)
        set(trevrpc_codegen_collision_script
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/codegen/check_collision.cmake")
        set(trevrpc_codegen_collision_include_dir
            "${CMAKE_CURRENT_SOURCE_DIR}/tests/codegen")

        add_test(NAME trevrpc_codegen_rejects_message_identifier_collision
            COMMAND "${CMAKE_COMMAND}"
                "-DPROTOC_EXECUTABLE=${PROTOC_EXECUTABLE}"
                "-DGENERATOR_EXECUTABLE=${TREVRPC_C_GENERATOR_EXECUTABLE}"
                "-DPROTO_INCLUDE_DIR=${trevrpc_codegen_collision_include_dir}"
                "-DPROTO_FILE=${CMAKE_CURRENT_SOURCE_DIR}/tests/codegen/message_identifier_collision.proto"
                "-DOUTPUT_DIR=${CMAKE_CURRENT_BINARY_DIR}/codegen-collision-message"
                "-DEXPECTED_ERROR=FooBar.*foo_bar.*duplicate C type"
                -P "${trevrpc_codegen_collision_script}"
        )
        add_test(NAME trevrpc_codegen_rejects_service_identifier_collision
            COMMAND "${CMAKE_COMMAND}"
                "-DPROTOC_EXECUTABLE=${PROTOC_EXECUTABLE}"
                "-DGENERATOR_EXECUTABLE=${TREVRPC_C_GENERATOR_EXECUTABLE}"
                "-DPROTO_INCLUDE_DIR=${trevrpc_codegen_collision_include_dir}"
                "-DPROTO_FILE=${CMAKE_CURRENT_SOURCE_DIR}/tests/codegen/service_identifier_collision.proto"
                "-DOUTPUT_DIR=${CMAKE_CURRENT_BINARY_DIR}/codegen-collision-service"
                "-DEXPECTED_ERROR=FooBar.*foo_bar.*duplicate C macro"
                -P "${trevrpc_codegen_collision_script}"
        )
        add_test(NAME trevrpc_codegen_rejects_method_identifier_collision
            COMMAND "${CMAKE_COMMAND}"
                "-DPROTOC_EXECUTABLE=${PROTOC_EXECUTABLE}"
                "-DGENERATOR_EXECUTABLE=${TREVRPC_C_GENERATOR_EXECUTABLE}"
                "-DPROTO_INCLUDE_DIR=${trevrpc_codegen_collision_include_dir}"
                "-DPROTO_FILE=${CMAKE_CURRENT_SOURCE_DIR}/tests/codegen/method_identifier_collision.proto"
                "-DOUTPUT_DIR=${CMAKE_CURRENT_BINARY_DIR}/codegen-collision-method"
                "-DEXPECTED_ERROR=FooBar.*foo_bar.*duplicate C macro"
                -P "${trevrpc_codegen_collision_script}"
        )
        add_test(NAME trevrpc_codegen_rejects_package_identifier_collision
            COMMAND "${CMAKE_COMMAND}"
                "-DPROTOC_EXECUTABLE=${PROTOC_EXECUTABLE}"
                "-DGENERATOR_EXECUTABLE=${TREVRPC_C_GENERATOR_EXECUTABLE}"
                "-DPROTO_INCLUDE_DIR=${trevrpc_codegen_collision_include_dir}"
                "-DPROTO_FILE=${CMAKE_CURRENT_SOURCE_DIR}/tests/codegen/package_identifier_collision_a.proto"
                "-DPROTO_FILE_2=${CMAKE_CURRENT_SOURCE_DIR}/tests/codegen/package_identifier_collision_b.proto"
                "-DOUTPUT_DIR=${CMAKE_CURRENT_BINARY_DIR}/codegen-collision-package"
                "-DEXPECTED_ERROR=foo\.bar\.Service\.Call.*foo_bar\.Service\.Call.*duplicate C macro"
                -P "${trevrpc_codegen_collision_script}"
        )
        add_test(NAME trevrpc_codegen_rejects_file_prefix_collision
            COMMAND "${CMAKE_COMMAND}"
                "-DPROTOC_EXECUTABLE=${PROTOC_EXECUTABLE}"
                "-DGENERATOR_EXECUTABLE=${TREVRPC_C_GENERATOR_EXECUTABLE}"
                "-DPROTO_INCLUDE_DIR=${trevrpc_codegen_collision_include_dir}"
                "-DPROTO_FILE=${CMAKE_CURRENT_SOURCE_DIR}/tests/codegen/a/foo.proto"
                "-DPROTO_FILE_2=${CMAKE_CURRENT_SOURCE_DIR}/tests/codegen/afoo.proto"
                "-DOUTPUT_DIR=${CMAKE_CURRENT_BINARY_DIR}/codegen-collision-file-prefix"
                "-DEXPECTED_ERROR=a/foo\.proto.*afoo\.proto.*duplicate C symbol"
                -P "${trevrpc_codegen_collision_script}"
        )
        add_test(NAME trevrpc_codegen_escapes_c_quote_newlines
            COMMAND "${CMAKE_COMMAND}"
                "-DPROTOC_EXECUTABLE=${PROTOC_EXECUTABLE}"
                "-DGENERATOR_EXECUTABLE=${TREVRPC_C_GENERATOR_EXECUTABLE}"
                "-DPROTO_INCLUDE_DIR=${trevrpc_codegen_collision_include_dir}"
                "-DPROTO_FILE=${CMAKE_CURRENT_SOURCE_DIR}/tests/codegen/c_quote.proto"
                "-DOUTPUT_DIR=${CMAKE_CURRENT_BINARY_DIR}/codegen-c-quote"
                -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/codegen/check_c_quote.cmake"
        )
    endif()

endif()
