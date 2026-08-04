if(NOT DEFINED TREVRPC_SOURCE_ROOT)
    message(FATAL_ERROR "TREVRPC_SOURCE_ROOT is required")
endif()

set(scan_roots
    "${TREVRPC_SOURCE_ROOT}/trevrpc-c/include"
    "${TREVRPC_SOURCE_ROOT}/trevrpc-c/src"
    "${TREVRPC_SOURCE_ROOT}/trevrpc-c/tests"
    "${TREVRPC_SOURCE_ROOT}/trevrpc-c/examples"
    "${TREVRPC_SOURCE_ROOT}/trevrpc-c/bench"
    "${TREVRPC_SOURCE_ROOT}/trevrpc-cpp/src"
    "${TREVRPC_SOURCE_ROOT}/trevrpc-cpp/tests"
    "${TREVRPC_SOURCE_ROOT}/trevrpc-js/native"
    "${TREVRPC_SOURCE_ROOT}/trevrpc-js/test"
    "${TREVRPC_SOURCE_ROOT}/conformance/adapters/c-family"
)

set(forbidden_tokens
    trevrpc_preview.h
    TREVRPC_C_PREVIEW_VERSION
    trevrpc_config
    trevrpc_server_config
    trevrpc_server_options
    trevrpc_call_options
    trevrpc_response
    trevrpc_stream_frame
    trevrpc_unary_handler
)

file(STRINGS "${CMAKE_CURRENT_LIST_DIR}/removed-symbols.txt" removed_symbols)
set(failures "")
foreach(root IN LISTS scan_roots)
    if(NOT EXISTS "${root}")
        continue()
    endif()
    file(GLOB_RECURSE files LIST_DIRECTORIES FALSE
        "${root}/*.c" "${root}/*.h" "${root}/*.cc" "${root}/*.cpp" "${root}/*.js")
    foreach(path IN LISTS files)
        if(path MATCHES "/tests/abi5/" OR path MATCHES "/tests/golden/")
            continue()
        endif()
        file(READ "${path}" text)
        foreach(token IN LISTS forbidden_tokens)
            string(REGEX MATCH "(^|[^A-Za-z0-9_])${token}([^A-Za-z0-9_]|$)" match "${text}")
            if(match)
                list(APPEND failures "${path}: forbidden ABI-5 token ${token}")
            endif()
        endforeach()
        foreach(symbol IN LISTS removed_symbols)
            string(REGEX MATCH "(^|[^A-Za-z0-9_])${symbol}[ \t\r\n]*\\(" match "${text}")
            if(match)
                list(APPEND failures "${path}: removed ABI-5 function ${symbol}")
            endif()
        endforeach()
    endforeach()
endforeach()

if(EXISTS "${TREVRPC_SOURCE_ROOT}/trevrpc-c/include/trevrpc_preview.h")
    list(APPEND failures "trevrpc_preview.h still exists")
endif()

if(failures)
    list(JOIN failures "\n" report)
    message(FATAL_ERROR "ABI-6 source surface check failed:\n${report}")
endif()
