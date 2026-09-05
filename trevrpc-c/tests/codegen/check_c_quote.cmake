if(NOT DEFINED PROTOC_EXECUTABLE OR NOT DEFINED GENERATOR_EXECUTABLE OR NOT DEFINED PROTO_INCLUDE_DIR OR NOT DEFINED PROTO_FILE OR NOT DEFINED OUTPUT_DIR)
    message(FATAL_ERROR "check_c_quote.cmake requires protoc, generator, proto, include, and output paths")
endif()

file(REMOVE_RECURSE "${OUTPUT_DIR}")
file(MAKE_DIRECTORY "${OUTPUT_DIR}")
string(ASCII 10 newline)
set(runtime_include "runtime${newline}include.h")
execute_process(
    COMMAND "${PROTOC_EXECUTABLE}"
        --plugin=protoc-gen-trevrpc-c=${GENERATOR_EXECUTABLE}
        --trevrpc-c_out=${OUTPUT_DIR}
        "--trevrpc-c_opt=runtime_include=${runtime_include}"
        -I "${PROTO_INCLUDE_DIR}"
        "${PROTO_FILE}"
    RESULT_VARIABLE result
    OUTPUT_VARIABLE stdout
    ERROR_VARIABLE stderr
)
if(NOT result EQUAL 0)
    message(FATAL_ERROR "protoc unexpectedly rejected c_quote input:\n${stdout}\n${stderr}")
endif()

file(READ "${OUTPUT_DIR}/c_quote.trevrpc.h" generated)
string(FIND "${generated}" "#include \"runtime\\ninclude.h\"" escaped_include)
if(escaped_include EQUAL -1)
    message(FATAL_ERROR "generated header did not contain an escaped newline in runtime include")
endif()
