if(NOT DEFINED NM OR NOT DEFINED BASELINE)
    message(FATAL_ERROR "NM and BASELINE are required")
endif()

set(archives
    "${ARCHIVE_CORE}"
    "${ARCHIVE_MSQUIC}"
    "${ARCHIVE_WEBTRANSPORT}"
    "${ARCHIVE_RUNTIME}"
)

execute_process(
    COMMAND "${NM}" -g --defined-only ${archives}
    RESULT_VARIABLE nm_result
    OUTPUT_VARIABLE nm_output
    ERROR_VARIABLE nm_error
)
if(NOT nm_result EQUAL 0)
    message(FATAL_ERROR "nm failed: ${nm_error}")
endif()

file(STRINGS "${BASELINE}" expected_symbols)
foreach(symbol IN LISTS expected_symbols)
    string(REGEX MATCH "(^|[\r\n])[^\r\n]*[ \t]${symbol}([\r\n]|$)" match "${nm_output}")
    if(NOT match)
        message(FATAL_ERROR "ABI-5 symbol is missing: ${symbol}")
    endif()
endforeach()
