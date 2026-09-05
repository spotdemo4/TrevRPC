if(NOT DEFINED PROTOC_EXECUTABLE OR NOT DEFINED GENERATOR_EXECUTABLE OR NOT DEFINED PROTO_INCLUDE_DIR OR NOT DEFINED OUTPUT_DIR)
    message(FATAL_ERROR "check_collision.cmake requires protoc, generator, include, and output paths")
endif()

set(proto_files)
if(DEFINED PROTO_FILE)
    list(APPEND proto_files "${PROTO_FILE}")
endif()
if(DEFINED PROTO_FILE_2)
    list(APPEND proto_files "${PROTO_FILE_2}")
endif()
if(NOT proto_files)
    message(FATAL_ERROR "check_collision.cmake requires at least one proto file")
endif()

file(REMOVE_RECURSE "${OUTPUT_DIR}")
file(MAKE_DIRECTORY "${OUTPUT_DIR}")
execute_process(
    COMMAND "${PROTOC_EXECUTABLE}"
        --plugin=protoc-gen-trevrpc-c=${GENERATOR_EXECUTABLE}
        --trevrpc-c_out=${OUTPUT_DIR}
        -I "${PROTO_INCLUDE_DIR}"
        ${proto_files}
    RESULT_VARIABLE result
    OUTPUT_VARIABLE stdout
    ERROR_VARIABLE stderr
)

if(result EQUAL 0)
    message(FATAL_ERROR "protoc unexpectedly accepted colliding declarations")
endif()
set(output "${stdout}\n${stderr}")
if(DEFINED EXPECTED_ERROR AND NOT output MATCHES "${EXPECTED_ERROR}")
    message(FATAL_ERROR "protoc rejected input, but output did not match '${EXPECTED_ERROR}':\n${output}")
endif()
