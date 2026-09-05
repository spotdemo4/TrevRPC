execute_process(
    COMMAND "${PEER}" server
        --stack trevrpc_webtransport
        --listen 127.0.0.1:0
        --cert unused-cert
        --key unused-key
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE diagnostics
)
if(result EQUAL 0 OR NOT output MATCHES
    "schema_version.:5.*code.:.invalid_argument.*requires --webtransport-origin")
    message(FATAL_ERROR
        "WebTransport server accepted a missing origin or returned an unexpected error:\n${diagnostics}${output}")
endif()

execute_process(
    COMMAND "${PEER}" server
        --stack trevrpc_native_quic
        --listen 127.0.0.1:0
        --cert unused-cert
        --key unused-key
        --webtransport-origin https://benchmark.invalid
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE diagnostics
)
if(result EQUAL 0 OR NOT output MATCHES
    "schema_version.:5.*code.:.invalid_argument.*only valid for a trevrpc_webtransport server")
    message(FATAL_ERROR
        "Native server accepted a WebTransport origin or returned an unexpected error:\n${diagnostics}${output}")
endif()

execute_process(
    COMMAND "${PEER}" client
        --stack trevrpc_http3
        --address 127.0.0.1:1
        --cert unused-cert
        --rpc unary
        --concurrency 1
        --warmup-ms 0
        --measurement-ms 1
        --request-bytes 0
        --response-bytes 0
        --messages-per-stream 1
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE diagnostics
)
if(result EQUAL 0 OR NOT output MATCHES
    "schema_version.:5.*code.:.invalid_argument.*trevrpc_http3 is server-only")
    message(FATAL_ERROR
        "HTTP/3 client accepted or returned an unexpected error:\n${diagnostics}${output}")
endif()

execute_process(
    COMMAND "${PEER}" client
        --stack trevrpc_webtransport
        --address 127.0.0.1:1
        --cert unused-cert
        --rpc unary
        --concurrency 1
        --warmup-ms 0
        --measurement-ms 1
        --request-bytes 0
        --response-bytes 0
        --messages-per-stream 1
    RESULT_VARIABLE result
    OUTPUT_VARIABLE output
    ERROR_VARIABLE diagnostics
)
if(result EQUAL 0 OR NOT output MATCHES
    "schema_version.:5.*code.:.invalid_argument.*trevrpc_webtransport is server-only")
    message(FATAL_ERROR
        "Client accepted WebTransport or returned an unexpected error:\n${diagnostics}${output}")
endif()
