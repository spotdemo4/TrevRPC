if(NOT DEFINED TREVRPC_NM OR NOT DEFINED TREVRPC_ARCHIVE OR NOT DEFINED TREVRPC_FORBIDDEN_MANIFEST)
    message(FATAL_ERROR "TREVRPC_NM, TREVRPC_ARCHIVE, and TREVRPC_FORBIDDEN_MANIFEST are required")
endif()

execute_process(
    COMMAND "${TREVRPC_NM}" -g --defined-only -P "${TREVRPC_ARCHIVE}"
    RESULT_VARIABLE nm_result
    OUTPUT_VARIABLE nm_output
    ERROR_VARIABLE nm_error
)
if(NOT nm_result EQUAL 0)
    message(FATAL_ERROR "nm failed: ${nm_error}")
endif()

file(STRINGS "${TREVRPC_FORBIDDEN_MANIFEST}" forbidden_symbols REGEX "^trevrpc_[A-Za-z0-9_]+$")
foreach(symbol IN LISTS forbidden_symbols)
    string(REGEX MATCH "(^|\\n)${symbol} [A-Za-z]" found "${nm_output}")
    if(found)
        message(FATAL_ERROR "canonical RPC private core defines forbidden ABI6 symbol ${symbol}")
    endif()
endforeach()
