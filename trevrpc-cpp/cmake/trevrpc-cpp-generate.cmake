function(trevrpc_cpp_generate)
  set(options)
  set(one_value_args TARGET PROTO IMPORT_DIR OUTPUT_DIR PROTOBUF_TARGET)
  cmake_parse_arguments(ARG "${options}" "${one_value_args}" "" ${ARGN})
  if(NOT ARG_TARGET OR NOT ARG_PROTO)
    message(FATAL_ERROR "trevrpc_cpp_generate requires TARGET and PROTO")
  endif()
  if(NOT ARG_IMPORT_DIR)
    get_filename_component(ARG_IMPORT_DIR "${ARG_PROTO}" DIRECTORY)
  endif()
  if(NOT ARG_OUTPUT_DIR)
    set(ARG_OUTPUT_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")
  endif()
  if(NOT ARG_PROTOBUF_TARGET)
    set(ARG_PROTOBUF_TARGET protobuf::libprotobuf)
  endif()

  file(REAL_PATH "${ARG_PROTO}" proto_path)
  file(REAL_PATH "${ARG_IMPORT_DIR}" import_path)
  file(RELATIVE_PATH proto_relative "${import_path}" "${proto_path}")
  if(proto_relative MATCHES "^\\.\\./" OR proto_relative STREQUAL "..")
    message(FATAL_ERROR "trevrpc_cpp_generate PROTO must be below IMPORT_DIR")
  endif()
  string(REGEX REPLACE "\\.proto$" "" proto_stem "${proto_relative}")
  if(proto_stem STREQUAL proto_relative)
    message(FATAL_ERROR "trevrpc_cpp_generate PROTO must end in .proto")
  endif()

  set(pb_cc "${ARG_OUTPUT_DIR}/${proto_stem}.pb.cc")
  set(rpc_cc "${ARG_OUTPUT_DIR}/${proto_stem}.trevrpc.cpp")
  set(depfile "${ARG_OUTPUT_DIR}/${proto_stem}.d")
  if(TARGET protoc-gen-trevrpc-cpp)
    set(generator_executable "$<TARGET_FILE:protoc-gen-trevrpc-cpp>")
    set(generator_dependency protoc-gen-trevrpc-cpp)
  else()
    find_program(generator_executable NAMES protoc-gen-trevrpc-cpp
      HINTS "${TREVRPC_CPP_BINDIR}" "${PACKAGE_PREFIX_DIR}/bin" REQUIRED)
    set(generator_dependency)
  endif()

  add_custom_command(
    OUTPUT
      "${pb_cc}"
      "${ARG_OUTPUT_DIR}/${proto_stem}.pb.h"
      "${rpc_cc}"
      "${ARG_OUTPUT_DIR}/${proto_stem}.trevrpc.hpp"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${ARG_OUTPUT_DIR}"
    COMMAND protobuf::protoc
      -I "${ARG_IMPORT_DIR}"
      --cpp_out=${ARG_OUTPUT_DIR}
      --dependency_out=${depfile}
      --plugin=protoc-gen-trevrpc-cpp=${generator_executable}
      --trevrpc-cpp_out=${ARG_OUTPUT_DIR}
      "${ARG_PROTO}"
    DEPENDS "${ARG_PROTO}" ${generator_dependency}
    DEPFILE "${depfile}"
    VERBATIM)
  add_library(${ARG_TARGET} STATIC "${pb_cc}" "${rpc_cc}")
  target_include_directories(${ARG_TARGET} PUBLIC "${ARG_OUTPUT_DIR}")
  target_compile_features(${ARG_TARGET} PUBLIC cxx_std_20)
  target_link_libraries(${ARG_TARGET} PUBLIC trevrpc::cpp ${ARG_PROTOBUF_TARGET})
endfunction()
