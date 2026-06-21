cmake_minimum_required(VERSION 3.20)

project(trevrpc-c LANGUAGES C)

include(GNUInstallDirs)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_C_EXTENSIONS OFF)

option(TREVRPC_BUILD_TESTS "Build trevrpc-c tests" ON)
option(TREVRPC_BUILD_MSQUIC "Build trevrpc-c MsQuic transport" ON)
option(TREVRPC_BUILD_WEBTRANSPORT "Build trevrpc-c WebTransport transport" ON)
option(TREVRPC_BUILD_RUNTIME "Build trevrpc-c high-level runtime" ON)

find_package(Threads REQUIRED)

add_library(trevrpc_core STATIC
    src/trevrpc_values.c
    src/trevrpc_wire.c
)
target_include_directories(trevrpc_core
    PUBLIC include
    PRIVATE src
)

function(trevrpc_c_warnings target)
    if(CMAKE_C_COMPILER_ID MATCHES "Clang|GNU")
        target_compile_options(${target} PRIVATE -Wall -Wextra -Werror)
    endif()
endfunction()

trevrpc_c_warnings(trevrpc_core)

set(trevrpc_install_targets trevrpc_core)

if(TREVRPC_BUILD_MSQUIC)
    find_path(MSQUIC_INCLUDE_DIR NAMES msquic.h inc/msquic.h REQUIRED)
    find_library(MSQUIC_LIBRARY NAMES msquic REQUIRED)

    add_library(trevrpc_msquic STATIC
        src/trevrpc_msquic.c
    )
    target_include_directories(trevrpc_msquic
        PUBLIC include
        PRIVATE ${MSQUIC_INCLUDE_DIR}
    )
    target_link_libraries(trevrpc_msquic PUBLIC Threads::Threads PRIVATE ${MSQUIC_LIBRARY})
    trevrpc_c_warnings(trevrpc_msquic)
    list(APPEND trevrpc_install_targets trevrpc_msquic)
endif()

if(TREVRPC_BUILD_WEBTRANSPORT)
    find_path(WTF_INCLUDE_DIR NAMES wtf.h REQUIRED)
    find_library(WTF_LIBRARY NAMES wtf REQUIRED)

    add_library(trevrpc_webtransport STATIC
        src/trevrpc_webtransport.c
    )
    target_include_directories(trevrpc_webtransport
        PUBLIC include
        PRIVATE ${WTF_INCLUDE_DIR}
    )
    target_link_libraries(trevrpc_webtransport PUBLIC Threads::Threads PRIVATE ${WTF_LIBRARY})
    trevrpc_c_warnings(trevrpc_webtransport)
    list(APPEND trevrpc_install_targets trevrpc_webtransport)
endif()

if(TREVRPC_BUILD_RUNTIME)
    if(NOT TREVRPC_BUILD_MSQUIC)
        message(FATAL_ERROR "TREVRPC_BUILD_RUNTIME requires TREVRPC_BUILD_MSQUIC because the high-level runtime is currently MsQuic-backed")
    endif()

    add_library(trevrpc STATIC
        src/trevrpc.c
    )
    target_include_directories(trevrpc
        PUBLIC include
        PRIVATE src
    )
    target_link_libraries(trevrpc PUBLIC trevrpc_core trevrpc_msquic Threads::Threads)
    trevrpc_c_warnings(trevrpc)
    list(APPEND trevrpc_install_targets trevrpc)
endif()

install(TARGETS ${trevrpc_install_targets}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
install(FILES
    include/trevrpc.h
    include/trevrpc_msquic.h
    include/trevrpc_webtransport.h
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)

if(TREVRPC_BUILD_TESTS)
    enable_testing()

    add_executable(trevrpc_wire_test tests/wire_test.c)
    target_include_directories(trevrpc_wire_test PRIVATE src)
    target_compile_definitions(trevrpc_wire_test PRIVATE TREVRPC_WIRE_GOLDEN_VECTORS="${CMAKE_CURRENT_SOURCE_DIR}/../testdata/wire-golden-vectors.txt")
    target_link_libraries(trevrpc_wire_test PRIVATE trevrpc_core)
    trevrpc_c_warnings(trevrpc_wire_test)

    add_test(NAME trevrpc_wire COMMAND trevrpc_wire_test)

    if(TREVRPC_BUILD_RUNTIME)
        find_program(PROTOC_EXECUTABLE protoc REQUIRED)
        find_program(PROTOC_C_EXECUTABLE protoc-c REQUIRED)
        find_program(TREVRPC_C_GENERATOR_EXECUTABLE protoc-gen-trevrpc-c)

        set(trevrpc_generated_test_dir "${CMAKE_CURRENT_BINARY_DIR}/generated-service-test")
        set(trevrpc_generated_proto "${CMAKE_CURRENT_SOURCE_DIR}/tests/proto/greeter.proto")
        set(trevrpc_generated_pb_c "${trevrpc_generated_test_dir}/greeter.pb-c.c")
        set(trevrpc_generated_trevrpc_c "${trevrpc_generated_test_dir}/greeter.trevrpc.c")

        if(NOT TREVRPC_C_GENERATOR_EXECUTABLE)
            find_program(GO_EXECUTABLE go REQUIRED)
            set(TREVRPC_C_GENERATOR_EXECUTABLE "${trevrpc_generated_test_dir}/protoc-gen-trevrpc-c")
            add_custom_command(
                OUTPUT "${TREVRPC_C_GENERATOR_EXECUTABLE}"
                COMMAND "${CMAKE_COMMAND}" -E make_directory "${trevrpc_generated_test_dir}"
                COMMAND "${CMAKE_COMMAND}" -E chdir "${CMAKE_CURRENT_SOURCE_DIR}/../trevrpc-go" "${GO_EXECUTABLE}" build -o "${TREVRPC_C_GENERATOR_EXECUTABLE}" ./cmd/protoc-gen-trevrpc-c
                DEPENDS "${CMAKE_CURRENT_SOURCE_DIR}/../trevrpc-go/cmd/protoc-gen-trevrpc-c/main.go"
                VERBATIM
            )
        endif()
        add_custom_command(
            OUTPUT
                "${trevrpc_generated_test_dir}/greeter.pb-c.c"
                "${trevrpc_generated_test_dir}/greeter.pb-c.h"
                "${trevrpc_generated_test_dir}/greeter.trevrpc.c"
                "${trevrpc_generated_test_dir}/greeter.trevrpc.h"
            COMMAND "${CMAKE_COMMAND}" -E make_directory "${trevrpc_generated_test_dir}"
            COMMAND "${PROTOC_C_EXECUTABLE}" --c_out=${trevrpc_generated_test_dir} -I "${CMAKE_CURRENT_SOURCE_DIR}/tests/proto" "${trevrpc_generated_proto}"
            COMMAND "${PROTOC_EXECUTABLE}" --plugin=protoc-gen-trevrpc-c=${TREVRPC_C_GENERATOR_EXECUTABLE} --trevrpc-c_out=${trevrpc_generated_test_dir} -I "${CMAKE_CURRENT_SOURCE_DIR}/tests/proto" "${trevrpc_generated_proto}"
            DEPENDS "${trevrpc_generated_proto}" "${TREVRPC_C_GENERATOR_EXECUTABLE}"
            VERBATIM
        )

        add_executable(trevrpc_runtime_test tests/runtime_test.c)
        target_include_directories(trevrpc_runtime_test PRIVATE src)
        target_compile_definitions(trevrpc PRIVATE TREVRPC_TESTING)
        target_link_libraries(trevrpc_runtime_test PRIVATE trevrpc)
        trevrpc_c_warnings(trevrpc_runtime_test)

        add_test(NAME trevrpc_runtime COMMAND trevrpc_runtime_test)

        add_executable(trevrpc_generated_service_test
            tests/generated_service_test.c
            "${trevrpc_generated_pb_c}"
            "${trevrpc_generated_trevrpc_c}"
        )
        target_include_directories(trevrpc_generated_service_test PRIVATE src "${trevrpc_generated_test_dir}")
        target_link_libraries(trevrpc_generated_service_test PRIVATE trevrpc protobuf-c)
        trevrpc_c_warnings(trevrpc_generated_service_test)

        add_test(NAME trevrpc_generated_service COMMAND trevrpc_generated_service_test)
    endif()
endif()
