if(NOT DEFINED PROTOC OR NOT DEFINED PLUGIN OR NOT DEFINED PROTO_DIR OR NOT DEFINED OUTPUT_DIR)
  message(FATAL_ERROR "generator collision test inputs are required")
endif()

file(MAKE_DIRECTORY "${OUTPUT_DIR}")
execute_process(
  COMMAND "${PROTOC}"
    -I "${PROTO_DIR}"
    "--plugin=protoc-gen-trevrpc-cpp=${PLUGIN}"
    "--trevrpc-cpp_out=${OUTPUT_DIR}"
    "${PROTO_DIR}/generator_collisions.proto"
  RESULT_VARIABLE result
  OUTPUT_VARIABLE output
  ERROR_VARIABLE error)
if(result EQUAL 0)
  message(FATAL_ERROR "collision schema unexpectedly generated successfully")
endif()
string(FIND "${error}" "generated C++ declaration collision" collision_index)
if(collision_index EQUAL -1)
  message(FATAL_ERROR "generator failed without the expected collision diagnostic: ${error}${output}")
endif()
