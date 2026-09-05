if(NOT DEFINED PEER OR NOT DEFINED ROLE)
  message(FATAL_ERROR "PEER and ROLE are required")
endif()

execute_process(
  COMMAND "${PEER}" "${ROLE}"
  RESULT_VARIABLE result
  OUTPUT_VARIABLE output
  ERROR_VARIABLE diagnostics)

if(result EQUAL 0)
  message(FATAL_ERROR "${ROLE} accepted a command without --stack")
endif()
if(NOT output MATCHES
    "schema_version.:5.*code.:.invalid_argument.*missing required option --stack")
  message(FATAL_ERROR
    "${ROLE} returned an unexpected error without --stack:\n${diagnostics}${output}")
endif()
