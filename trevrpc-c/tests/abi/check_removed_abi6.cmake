if(NOT DEFINED TREVRPC_NM OR NOT DEFINED TREVRPC_AR)
    message(FATAL_ERROR "TREVRPC_NM and TREVRPC_AR are required")
endif()

set(removed_msquic_manifest "${CMAKE_CURRENT_LIST_DIR}/removed-msquic-symbols.txt")
file(STRINGS "${removed_msquic_manifest}" removed_msquic_symbols)
list(FILTER removed_msquic_symbols EXCLUDE REGEX "^[ \t]*(#|$)")
list(LENGTH removed_msquic_symbols removed_msquic_symbol_count)
set(unique_removed_msquic_symbols ${removed_msquic_symbols})
list(REMOVE_DUPLICATES unique_removed_msquic_symbols)
list(LENGTH unique_removed_msquic_symbols unique_removed_msquic_symbol_count)
if(NOT removed_msquic_symbol_count EQUAL 42 OR NOT unique_removed_msquic_symbol_count EQUAL 42)
    message(FATAL_ERROR "former MsQuic symbol manifest must contain exactly 42 unique entries")
endif()

set(removed_abi6_manifest "${CMAKE_CURRENT_LIST_DIR}/removed-abi6-symbols.txt")
file(STRINGS "${removed_abi6_manifest}" removed_abi6_symbols)
list(FILTER removed_abi6_symbols EXCLUDE REGEX "^[ \t]*(#|$)")
list(LENGTH removed_abi6_symbols removed_abi6_symbol_count)
set(unique_removed_abi6_symbols ${removed_abi6_symbols})
list(REMOVE_DUPLICATES unique_removed_abi6_symbols)
list(LENGTH unique_removed_abi6_symbols unique_removed_abi6_symbol_count)
if(NOT removed_abi6_symbol_count EQUAL 266 OR NOT unique_removed_abi6_symbol_count EQUAL 266)
    message(FATAL_ERROR "ABI 6 tombstone manifest must contain exactly 266 unique entries")
endif()

set(removed_abi_symbols
    ${removed_abi6_symbols}
    ${removed_msquic_symbols}
    TREVRPC_C_ABI_VERSION
    trevrpc_msquic_abi6_shim_archive_anchor
)
list(REMOVE_DUPLICATES removed_abi_symbols)
set(removed_headers
    trevrpc.h
    trevrpc_binding.h
    trevrpc_msquic.h
    trevrpc_raw.h
    trevrpc_webtransport.h
)
set(removed_archive_member_pattern
    "(trevrpc|trevrpc_(channel|lifetime|msquic|ownership|versioned_runtime|values_abi6|wire_abi6|webtransport))\\.")

set(archives "")
if(DEFINED TREVRPC_ARCHIVES)
    list(APPEND archives ${TREVRPC_ARCHIVES})
endif()
if(DEFINED TREVRPC_INSTALL_PREFIXES)
    foreach(prefix IN LISTS TREVRPC_INSTALL_PREFIXES)
        file(GLOB_RECURSE installed_archives LIST_DIRECTORIES false "${prefix}/*.a")
        list(APPEND archives ${installed_archives})
        foreach(header IN LISTS removed_headers)
            if(EXISTS "${prefix}/include/${header}")
                message(FATAL_ERROR "removed ABI 6 header is installed: ${prefix}/include/${header}")
            endif()
        endforeach()
        foreach(path IN ITEMS
            "${prefix}/lib/libtrevrpc.a"
            "${prefix}/lib/libtrevrpc_core.a"
            "${prefix}/lib/libtrevrpc_msquic.a"
            "${prefix}/lib/libtrevrpc_msquic_native_core.a"
            "${prefix}/lib/libtrevrpc_protocol_core.a"
            "${prefix}/lib/libtrevrpc_webtransport.a"
            "${prefix}/lib/pkgconfig/trevrpc.pc"
            "${prefix}/lib/pkgconfig/trevrpc_core.pc"
            "${prefix}/lib/pkgconfig/trevrpc_msquic.pc"
            "${prefix}/lib/pkgconfig/trevrpc_webtransport.pc"
            "${prefix}/lib/cmake/trevrpc")
            if(EXISTS "${path}")
                message(FATAL_ERROR "removed ABI 6 package artifact is installed: ${path}")
            endif()
        endforeach()
    endforeach()
endif()
list(REMOVE_DUPLICATES archives)

if(NOT archives)
    message(FATAL_ERROR "no archives were supplied or found")
endif()

foreach(archive IN LISTS archives)
    if(NOT EXISTS "${archive}")
        message(FATAL_ERROR "archive does not exist: ${archive}")
    endif()
    execute_process(
        COMMAND "${TREVRPC_NM}" -g "${archive}"
        RESULT_VARIABLE nm_result
        OUTPUT_VARIABLE nm_output
        ERROR_VARIABLE nm_error
    )
    if(NOT nm_result EQUAL 0)
        message(FATAL_ERROR "nm failed for ${archive}: ${nm_error}")
    endif()
    foreach(symbol IN LISTS removed_abi_symbols)
        if(nm_output MATCHES "(^|[^A-Za-z0-9_])_?${symbol}([^A-Za-z0-9_]|$)")
            message(FATAL_ERROR "removed ABI 6 symbol ${symbol} is referenced by ${archive}")
        endif()
    endforeach()

    execute_process(
        COMMAND "${TREVRPC_AR}" t "${archive}"
        RESULT_VARIABLE ar_result
        OUTPUT_VARIABLE ar_output
        ERROR_VARIABLE ar_error
    )
    if(NOT ar_result EQUAL 0)
        message(FATAL_ERROR "ar failed for ${archive}: ${ar_error}")
    endif()
    string(REPLACE "\n" ";" archive_members "${ar_output}")
    foreach(member IN LISTS archive_members)
        if(member MATCHES "[Aa][Bb][Ii].?6|abi6_bridge" OR member MATCHES "${removed_archive_member_pattern}")
            message(FATAL_ERROR "removed ABI 6 object remains in ${archive}: ${member}")
        endif()
    endforeach()
endforeach()

if(DEFINED TREVRPC_SOURCE_DIR)
    foreach(header IN LISTS removed_headers)
        if(EXISTS "${TREVRPC_SOURCE_DIR}/include/${header}")
            message(FATAL_ERROR "removed ABI 6 header remains in the source tree: include/${header}")
        endif()
    endforeach()

    file(GLOB_RECURSE source_files LIST_DIRECTORIES false
        "${TREVRPC_SOURCE_DIR}/include/*.h"
        "${TREVRPC_SOURCE_DIR}/src/*.c"
        "${TREVRPC_SOURCE_DIR}/src/*.h")
    foreach(source_file IN LISTS source_files)
        file(READ "${source_file}" source_text)
        foreach(symbol IN LISTS removed_abi_symbols)
            if(source_text MATCHES "(^|[^A-Za-z0-9_])${symbol}([^A-Za-z0-9_]|$)")
                message(FATAL_ERROR "removed ABI 6 name ${symbol} remains in ${source_file}")
            endif()
        endforeach()
    endforeach()
endif()
