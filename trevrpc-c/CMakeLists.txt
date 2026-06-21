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
endif()
