cmake_minimum_required(VERSION 3.20)

project(trevrpc-c VERSION 0.2.2 LANGUAGES C)

set(TREVRPC_C_ABI_VERSION 6)
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
option(TREVRPC_BUILD_MSQUIC "Build the internal legacy ABI 6 MsQuic compatibility transport" OFF)
option(TREVRPC_BUILD_WEBTRANSPORT "Build the internal legacy ABI 6 WebTransport compatibility transport" OFF)
option(TREVRPC_BUILD_RUNTIME "Build the internal legacy trevrpc-c ABI 6 compatibility runtime" OFF)
option(TREVRPC_BUILD_CODEGEN "Build protoc-gen-trevrpc-c" "${trevrpc_default_build_codegen}")
set(trevrpc_default_build_rpc_test_support OFF)
if(TREVRPC_BUILD_TESTS AND TREVRPC_BUILD_RPC)
    set(trevrpc_default_build_rpc_test_support ON)
endif()
option(TREVRPC_BUILD_RPC_TEST_SUPPORT
    "Build the private RPC fake transport support library"
    "${trevrpc_default_build_rpc_test_support}"
)
option(TREVRPC_INSTALL_INTERNAL_HEADERS "Install trevrpc-c internal headers for tests and development" OFF)
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
set(TREVRPC_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/trevrpc" CACHE PATH
    "Install directory for trevrpc CMake package files")
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

if(TREVRPC_BUILD_MSQUIC OR TREVRPC_BUILD_RPC OR TREVRPC_BUILD_TRANSPORT_MSQUIC)
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

if(TARGET trevrpc_core)
    trevrpc_c_warnings(trevrpc_core)
    trevrpc_c_sanitizers(trevrpc_core)
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
        PRIVATE src
    )
    target_link_libraries(trevrpc_rpc
        PUBLIC Threads::Threads
        PRIVATE
            $<BUILD_INTERFACE:trevrpc_core>
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
    target_include_directories(trevrpc_rpc_fake_transport_support PRIVATE src tests)
    target_link_libraries(trevrpc_rpc_fake_transport_support PUBLIC trevrpc_rpc)
    trevrpc_c_warnings(trevrpc_rpc_fake_transport_support)
    trevrpc_c_sanitizers(trevrpc_rpc_fake_transport_support)
endif()

set(trevrpc_install_targets)
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

if(TARGET trevrpc_core)
    list(APPEND trevrpc_install_targets trevrpc_core)
    if(TREVRPC_BUILD_MSQUIC)
        list(APPEND trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_core.pc")
        configure_file(cmake/trevrpc_core.pc.in "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_core.pc" @ONLY)
    endif()
endif()
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

if(TREVRPC_BUILD_MSQUIC OR TREVRPC_BUILD_ENGINE_MSQUIC OR TREVRPC_BUILD_TRANSPORT_MSQUIC)
    find_package(msquic CONFIG REQUIRED)

    add_library(trevrpc_msquic_api_owner STATIC src/trevrpc_msquic_api_owner.c)
    target_include_directories(trevrpc_msquic_api_owner PRIVATE src)
    target_link_libraries(trevrpc_msquic_api_owner PRIVATE msquic::msquic Threads::Threads)
    trevrpc_c_warnings(trevrpc_msquic_api_owner)
    trevrpc_c_sanitizers(trevrpc_msquic_api_owner)
endif()

if(TREVRPC_BUILD_MSQUIC OR TREVRPC_BUILD_TRANSPORT_MSQUIC)
    if(TREVRPC_BUILD_MSQUIC)
        list(APPEND trevrpc_install_targets trevrpc_msquic_api_owner)
    endif()

    add_library(trevrpc_protocol_core STATIC
        src/trevrpc_quic_varint.c
        src/trevrpc_qpack_static.c
        src/trevrpc_http3_frame.c
        src/trevrpc_http3_headers.c
        src/trevrpc_http3_settings.c
        src/trevrpc_h3_demux.c
        src/trevrpc_h3_ingress.c
        src/trevrpc_msquic_features.c
        src/trevrpc_webtransport_profile.c
        src/trevrpc_webtransport_capsule.c
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
        src/trevrpc_msquic_core.c
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
    if(TREVRPC_BUILD_MSQUIC)
        list(APPEND trevrpc_install_targets trevrpc_msquic_native_core)
    endif()

    if(TREVRPC_BUILD_TESTS)
        add_library(trevrpc_msquic_testing STATIC
            src/trevrpc_msquic_core.c
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

    if(TREVRPC_BUILD_MSQUIC)
        add_library(trevrpc_msquic STATIC src/trevrpc_msquic.c)
        target_include_directories(trevrpc_msquic
            PUBLIC
                $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        )
        target_link_libraries(trevrpc_msquic
            PUBLIC trevrpc_msquic_native_core
            PRIVATE trevrpc_core
        )
        trevrpc_c_warnings(trevrpc_msquic)
        trevrpc_c_sanitizers(trevrpc_msquic)

        list(APPEND trevrpc_install_targets trevrpc_msquic)
        list(APPEND trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_msquic.pc")
        configure_file(cmake/trevrpc_msquic.pc.in "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_msquic.pc" @ONLY)
    endif()
endif()

if(TREVRPC_BUILD_ENGINE_MSQUIC)
    add_library(trevrpc_engine_msquic_support STATIC src/trevrpc_frame.c)
    target_include_directories(trevrpc_engine_msquic_support PRIVATE include src)
    trevrpc_c_warnings(trevrpc_engine_msquic_support)
    trevrpc_c_sanitizers(trevrpc_engine_msquic_support)

    add_library(trevrpc_engine_msquic STATIC src/trevrpc_engine_msquic.c)
    target_include_directories(trevrpc_engine_msquic
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
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
        src/trevrpc_rpc_transport_engine.c
        src/trevrpc_rpc_transport_h3.c
        src/trevrpc_rpc_transport_msquic.c
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

    add_library(trevrpc_transport_msquic STATIC src/trevrpc_transport_msquic.c)
    target_include_directories(trevrpc_transport_msquic
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
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

    list(APPEND trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_transport_msquic.pc")
    configure_file(cmake/trevrpc_transport_msquic.pc.in
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_transport_msquic.pc" @ONLY)
endif()

if(TREVRPC_BUILD_RPC_MSQUIC)
    add_library(trevrpc_rpc_msquic STATIC src/trevrpc_rpc_msquic.c)
    target_include_directories(trevrpc_rpc_msquic
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
        PRIVATE src
    )
    target_link_libraries(trevrpc_rpc_msquic PUBLIC trevrpc_rpc trevrpc_transport_msquic)
    trevrpc_c_warnings(trevrpc_rpc_msquic)
    trevrpc_c_sanitizers(trevrpc_rpc_msquic)

    list(APPEND trevrpc_pkg_config_files "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_rpc_msquic.pc")
    configure_file(cmake/trevrpc_rpc_msquic.pc.in
        "${CMAKE_CURRENT_BINARY_DIR}/trevrpc_rpc_msquic.pc" @ONLY)
endif()

if(TREVRPC_BUILD_WEBTRANSPORT)
    if(NOT TREVRPC_BUILD_MSQUIC)
        message(FATAL_ERROR "TREVRPC_BUILD_WEBTRANSPORT requires TREVRPC_BUILD_MSQUIC")
    endif()

    set(trevrpc_webtransport_sources
        src/trevrpc_webtransport.c
    )
    add_library(trevrpc_webtransport STATIC ${trevrpc_webtransport_sources})
    target_include_directories(trevrpc_webtransport
        PUBLIC
            $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
    )
    target_link_libraries(trevrpc_webtransport
        PUBLIC trevrpc_msquic Threads::Threads
        PRIVATE
            $<BUILD_INTERFACE:trevrpc_protocol_core>
            $<INSTALL_INTERFACE:-ltrevrpc_protocol_core>
            trevrpc_core
    )
    target_link_directories(trevrpc_webtransport INTERFACE
        $<INSTALL_INTERFACE:${CMAKE_INSTALL_LIBDIR}>
    )
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
            ${trevrpc_webtransport_sources}
        )
        target_compile_definitions(trevrpc_msquic_testing_runtime PRIVATE TREVRPC_H3_FRAME_POLICY_TESTING)
        trevrpc_c_configure_runtime(trevrpc_msquic_testing_runtime trevrpc_msquic_testing)
        target_link_libraries(trevrpc_msquic_testing_runtime PRIVATE trevrpc_protocol_core)
    endif()
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

if((TREVRPC_BUILD_RUNTIME AND (TREVRPC_BUILD_TESTS OR TREVRPC_BUILD_BENCHMARKS))
    OR (TREVRPC_BUILD_ENGINE_MSQUIC AND TREVRPC_BUILD_TESTS)
    OR (TREVRPC_BUILD_RPC_MSQUIC AND TREVRPC_BUILD_EXAMPLES))
    find_program(OPENSSL_EXECUTABLE openssl REQUIRED)
    set(trevrpc_msquic_test_cert "${CMAKE_CURRENT_BINARY_DIR}/msquic-test-cert.pem")
    set(trevrpc_msquic_test_key "${CMAKE_CURRENT_BINARY_DIR}/msquic-test-key.pem")
    add_custom_command(
        OUTPUT "${trevrpc_msquic_test_cert}" "${trevrpc_msquic_test_key}"
        COMMAND ${OPENSSL_EXECUTABLE} req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 3650 -subj /CN=localhost -addext "basicConstraints=critical,CA:FALSE" -addext "keyUsage=critical,digitalSignature" -addext "extendedKeyUsage=serverAuth" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" -keyout "${trevrpc_msquic_test_key}" -out "${trevrpc_msquic_test_cert}"
        VERBATIM
    )
    add_custom_target(trevrpc_msquic_test_certificates
        DEPENDS "${trevrpc_msquic_test_cert}" "${trevrpc_msquic_test_key}"
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
    if(NOT TREVRPC_BUILD_MSQUIC)
        install(TARGETS trevrpc_msquic_api_owner
            ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        )
    endif()
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
    if(TREVRPC_BUILD_RPC)
        install(FILES $<TARGET_FILE:trevrpc_core>
            DESTINATION ${TREVRPC_RPC_PRIVATE_INSTALL_LIBDIR}
            RENAME "${CMAKE_STATIC_LIBRARY_PREFIX}trevrpc_rpc_private_core${CMAKE_STATIC_LIBRARY_SUFFIX}"
        )
    endif()
endif()

if(TREVRPC_BUILD_MSQUIC)
    install(TARGETS trevrpc_protocol_core
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    )
    install(TARGETS ${trevrpc_install_targets}
        EXPORT trevrpcTargets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
    )
    install(EXPORT trevrpcTargets
        NAMESPACE trevrpc::
        DESTINATION "${TREVRPC_INSTALL_CMAKEDIR}"
    )
    if(TREVRPC_BUILD_ENGINE)
        set(TREVRPC_ENGINE_FIND_DEPENDENCY "find_dependency(trevrpc_engine CONFIG)")
    else()
        set(TREVRPC_ENGINE_FIND_DEPENDENCY "")
    endif()
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
if(trevrpc_pkg_config_files)
    install(FILES ${trevrpc_pkg_config_files}
        DESTINATION "${TREVRPC_INSTALL_PKGCONFIGDIR}"
    )
endif()
if(TREVRPC_BUILD_ENGINE)
    install(FILES include/trevrpc_engine.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )
endif()
if(TREVRPC_BUILD_ENGINE_MSQUIC)
    install(FILES include/trevrpc_engine_msquic.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )
endif()
if(TREVRPC_BUILD_TRANSPORT)
    install(FILES include/trevrpc_transport.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )
endif()
if(TREVRPC_BUILD_TRANSPORT_MSQUIC)
    install(FILES include/trevrpc_transport_msquic.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )
endif()
if(TREVRPC_BUILD_RPC)
    install(FILES include/trevrpc_rpc.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )
endif()
if(TREVRPC_BUILD_RPC_MSQUIC)
    install(FILES include/trevrpc_rpc_msquic.h
        DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    )
endif()
if(TREVRPC_BUILD_MSQUIC)
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

    add_executable(trevrpc_h3_demux_test tests/h3_demux_test.c)
    if(TARGET trevrpc_protocol_core)
        target_link_libraries(trevrpc_h3_demux_test PRIVATE trevrpc_protocol_core)
    else()
        target_sources(trevrpc_h3_demux_test PRIVATE
            src/trevrpc_h3_demux.c
            src/trevrpc_msquic_features.c
            src/trevrpc_quic_varint.c
            src/trevrpc_webtransport_profile.c
        )
    endif()
    target_include_directories(trevrpc_h3_demux_test PRIVATE src)
    trevrpc_c_warnings(trevrpc_h3_demux_test)
    trevrpc_c_sanitizers(trevrpc_h3_demux_test)
    add_test(NAME trevrpc_h3_demux COMMAND trevrpc_h3_demux_test)

    add_executable(trevrpc_h3_ingress_test
        tests/h3_ingress_test.c
        src/trevrpc_h3_ingress.c
    )
    if(TARGET trevrpc_protocol_core)
        target_link_libraries(trevrpc_h3_ingress_test PRIVATE trevrpc_protocol_core)
    else()
        target_sources(trevrpc_h3_ingress_test PRIVATE
            src/trevrpc_h3_demux.c
            src/trevrpc_msquic_features.c
            src/trevrpc_quic_varint.c
            src/trevrpc_webtransport_profile.c
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
            src/trevrpc_http3_frame.c
            src/trevrpc_quic_varint.c
        )
    endif()
    target_include_directories(trevrpc_http3_frame_test PRIVATE src)
    trevrpc_c_warnings(trevrpc_http3_frame_test)
    trevrpc_c_sanitizers(trevrpc_http3_frame_test)
    add_test(NAME trevrpc_http3_frame COMMAND trevrpc_http3_frame_test)

    if(TREVRPC_BUILD_RUNTIME)
        add_executable(trevrpc_http3_frame_policy_test tests/http3_frame_policy_test.c)
        target_include_directories(trevrpc_http3_frame_policy_test PRIVATE include src)
        target_link_libraries(trevrpc_http3_frame_policy_test PRIVATE trevrpc_msquic_testing_runtime)
        trevrpc_c_warnings(trevrpc_http3_frame_policy_test)
        trevrpc_c_sanitizers(trevrpc_http3_frame_policy_test)
        add_test(NAME trevrpc_http3_frame_policy COMMAND trevrpc_http3_frame_policy_test)
    endif()

    add_executable(trevrpc_http3_headers_test tests/http3_headers_test.c)
    if(TARGET trevrpc_protocol_core)
        target_link_libraries(trevrpc_http3_headers_test PRIVATE trevrpc_protocol_core)
    else()
        target_sources(trevrpc_http3_headers_test PRIVATE src/trevrpc_http3_headers.c)
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
            src/trevrpc_http3_settings.c
            src/trevrpc_quic_varint.c
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
        target_sources(trevrpc_qpack_static_test PRIVATE src/trevrpc_qpack_static.c)
    endif()
    target_include_directories(trevrpc_qpack_static_test PRIVATE src)
    trevrpc_c_warnings(trevrpc_qpack_static_test)
    trevrpc_c_sanitizers(trevrpc_qpack_static_test)
    add_test(NAME trevrpc_qpack_static COMMAND trevrpc_qpack_static_test)

    add_executable(trevrpc_quic_varint_test tests/quic_varint_test.c)
    if(TARGET trevrpc_protocol_core)
        target_link_libraries(trevrpc_quic_varint_test PRIVATE trevrpc_protocol_core)
    else()
        target_sources(trevrpc_quic_varint_test PRIVATE src/trevrpc_quic_varint.c)
    endif()
    target_include_directories(trevrpc_quic_varint_test PRIVATE src)
    trevrpc_c_warnings(trevrpc_quic_varint_test)
    trevrpc_c_sanitizers(trevrpc_quic_varint_test)
    add_test(NAME trevrpc_quic_varint COMMAND trevrpc_quic_varint_test)

    add_executable(trevrpc_msquic_features_test tests/msquic_features_test.c)
    if(TARGET trevrpc_protocol_core)
        target_link_libraries(trevrpc_msquic_features_test PRIVATE trevrpc_protocol_core)
    else()
        target_sources(trevrpc_msquic_features_test PRIVATE src/trevrpc_msquic_features.c)
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
            src/trevrpc_msquic_features.c
            src/trevrpc_webtransport_profile.c
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
            src/trevrpc_webtransport_capsule.c
            src/trevrpc_quic_varint.c
        )
    endif()
    target_include_directories(trevrpc_webtransport_capsule_test PRIVATE src)
    trevrpc_c_warnings(trevrpc_webtransport_capsule_test)
    trevrpc_c_sanitizers(trevrpc_webtransport_capsule_test)
    add_test(NAME trevrpc_webtransport_capsule COMMAND trevrpc_webtransport_capsule_test)

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

        add_test(NAME trevrpc_engine_abi1_symbols
            COMMAND "${CMAKE_COMMAND}"
                -DTREVRPC_NM=${CMAKE_NM}
                "-DTREVRPC_ARCHIVES=$<TARGET_FILE:trevrpc_engine>"
                "-DTREVRPC_PUBLIC_HEADERS=${CMAKE_CURRENT_SOURCE_DIR}/include/trevrpc_engine.h"
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

        add_executable(trevrpc_engine_dispatch_test tests/engine_dispatch_test.c)
        target_include_directories(trevrpc_engine_dispatch_test PRIVATE src)
        target_link_libraries(trevrpc_engine_dispatch_test PRIVATE trevrpc_engine)
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
                "-DTREVRPC_PUBLIC_HEADERS=${CMAKE_CURRENT_SOURCE_DIR}/include/trevrpc_engine_msquic.h"
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
        target_include_directories(trevrpc_engine_msquic_backpressure_test PRIVATE include src)
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

        add_test(NAME trevrpc_transport_abi1_symbols
            COMMAND "${CMAKE_COMMAND}"
                -DTREVRPC_NM=${CMAKE_NM}
                "-DTREVRPC_ARCHIVES=$<TARGET_FILE:trevrpc_transport>"
                "-DTREVRPC_PUBLIC_HEADERS=${CMAKE_CURRENT_SOURCE_DIR}/include/trevrpc_transport.h"
                -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/transport-abi1/check_symbols.cmake"
        )
    endif()

    if(TARGET trevrpc_transport_msquic)
        add_test(NAME trevrpc_transport_msquic_abi1_symbols
            COMMAND "${CMAKE_COMMAND}"
                -DTREVRPC_NM=${CMAKE_NM}
                "-DTREVRPC_ARCHIVES=$<TARGET_FILE:trevrpc_transport_msquic>"
                "-DTREVRPC_PUBLIC_HEADERS=${CMAKE_CURRENT_SOURCE_DIR}/include/trevrpc_transport_msquic.h"
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
            TREVRPC_MSQUIC_TEST_CERT="${trevrpc_msquic_test_cert}"
            TREVRPC_MSQUIC_TEST_KEY="${trevrpc_msquic_test_key}"
        )
        target_link_libraries(trevrpc_transport_native_roundtrip_test PRIVATE trevrpc_transport_msquic)
        trevrpc_c_warnings(trevrpc_transport_native_roundtrip_test)
        trevrpc_c_sanitizers(trevrpc_transport_native_roundtrip_test)
        add_test(NAME trevrpc_transport_native_roundtrip COMMAND trevrpc_transport_native_roundtrip_test)
        set_tests_properties(trevrpc_transport_native_roundtrip PROPERTIES TIMEOUT 30)
        trevrpc_c_msquic_loopback_test(trevrpc_transport_native_roundtrip)
        trevrpc_c_lsan_suppressions(trevrpc_transport_native_roundtrip)
        trevrpc_c_tsan_suppressions(trevrpc_transport_native_roundtrip)
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

        add_executable(trevrpc_rpc_fake_transport_test tests/rpc_fake_transport_test.c)
        target_include_directories(trevrpc_rpc_fake_transport_test PRIVATE src)
        target_link_libraries(trevrpc_rpc_fake_transport_test PRIVATE trevrpc_rpc_fake_transport_support)
        trevrpc_c_warnings(trevrpc_rpc_fake_transport_test)
        trevrpc_c_sanitizers(trevrpc_rpc_fake_transport_test)
        add_test(NAME trevrpc_rpc_fake_transport COMMAND trevrpc_rpc_fake_transport_test)
        set_tests_properties(trevrpc_rpc_fake_transport PROPERTIES TIMEOUT 30)

        add_test(NAME trevrpc_rpc_abi1_symbols
            COMMAND "${CMAKE_COMMAND}"
                -DTREVRPC_NM=${CMAKE_NM}
                -DTREVRPC_ARCHIVES=$<TARGET_FILE:trevrpc_rpc>
                -DTREVRPC_PUBLIC_HEADERS=${CMAKE_CURRENT_SOURCE_DIR}/include/trevrpc_rpc.h
                -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/rpc-abi1/check_symbols.cmake"
        )
    endif()

    if(TARGET trevrpc_rpc_msquic)
        add_test(NAME trevrpc_rpc_msquic_abi1_symbols
            COMMAND "${CMAKE_COMMAND}"
                -DTREVRPC_NM=${CMAKE_NM}
                -DTREVRPC_ARCHIVES=$<TARGET_FILE:trevrpc_rpc_msquic>
                -DTREVRPC_PUBLIC_HEADERS=${CMAKE_CURRENT_SOURCE_DIR}/include/trevrpc_rpc_msquic.h
                -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/rpc-abi1/check_msquic_symbols.cmake"
        )

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
            src/trevrpc_rpc_transport_h3.c
            src/trevrpc_rpc_transport_msquic.c
        )
        target_compile_definitions(trevrpc_rpc_transport_h3_test PRIVATE
            TREVRPC_MSQUIC_TESTING
            TREVRPC_RPC_TRANSPORT_H3_TESTING
        )
        target_include_directories(trevrpc_rpc_transport_h3_test PRIVATE include src)
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
        target_include_directories(trevrpc_rpc_transport_msquic_lifecycle_test PRIVATE include src tests)
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
            TREVRPC_MSQUIC_TEST_CERT="${trevrpc_msquic_test_cert}"
            TREVRPC_MSQUIC_TEST_KEY="${trevrpc_msquic_test_key}"
        )
        target_link_libraries(trevrpc_rpc_native_roundtrip_test PRIVATE trevrpc_rpc_msquic)
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
        target_link_libraries(trevrpc_rpc_native_shapes_test PRIVATE trevrpc_rpc_msquic)
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
        target_link_libraries(trevrpc_rpc_h3_shapes_test PRIVATE trevrpc_rpc_msquic)
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
        target_link_libraries(trevrpc_rpc_wt_shapes_test PRIVATE trevrpc_rpc_msquic)
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

    add_test(NAME trevrpc_abi6_source_surface
        COMMAND "${CMAKE_COMMAND}"
            -DTREVRPC_SOURCE_ROOT=${CMAKE_CURRENT_SOURCE_DIR}/..
            -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/abi6/check_surface.cmake"
    )

    if(TARGET trevrpc_core)
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
                "\"schema_version\":5.*\"roles\":\\{\"client\":\\[\"trevrpc_native_quic\"\\],\"server\":\\[\"trevrpc_native_quic\",\"trevrpc_http3\",\"trevrpc_webtransport\"\\]\\}.*\"rpc_kinds\":\\[\"unary\",\"client_stream\",\"server_stream\",\"bidi\"\\].*\"histogram\":\"log_linear_v1\""
        )
        add_test(NAME trevrpc_bench_peer_native_smoke
            COMMAND sh "${CMAKE_CURRENT_SOURCE_DIR}/tests/benchmark_peer_native_smoke.sh"
                $<TARGET_FILE:trevrpc_bench_peer_c>
                "${trevrpc_msquic_test_cert}"
                "${trevrpc_msquic_test_key}"
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
        )
        set_tests_properties(trevrpc_bench_peer_webtransport_smoke PROPERTIES TIMEOUT 30)
        trevrpc_c_msquic_loopback_test(trevrpc_bench_peer_webtransport_smoke)
        trevrpc_c_lsan_suppressions(trevrpc_bench_peer_webtransport_smoke)
        trevrpc_c_tsan_suppressions(trevrpc_bench_peer_webtransport_smoke)
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
                "-DTREVRPC_ARCHIVES=$<TARGET_FILE:trevrpc>;$<TARGET_FILE:trevrpc_core>;$<TARGET_FILE:trevrpc_msquic>;$<TARGET_FILE:trevrpc_msquic_native_core>;$<TARGET_FILE:trevrpc_protocol_core>;$<TARGET_FILE:trevrpc_webtransport>"
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
        target_link_libraries(trevrpc_runtime_test PRIVATE trevrpc_testing msquic::msquic)
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
        trevrpc_c_msquic_loopback_test(trevrpc_msquic_transport)
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
        trevrpc_c_msquic_loopback_test(trevrpc_channel)
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
            trevrpc_c_msquic_loopback_test(trevrpc_buffering_memory_${trevrpc_buffering_name})
            trevrpc_c_lsan_suppressions(trevrpc_buffering_memory_${trevrpc_buffering_name})
            trevrpc_c_tsan_suppressions(trevrpc_buffering_memory_${trevrpc_buffering_name})
        endforeach()

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
