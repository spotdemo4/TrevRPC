cmake_minimum_required(VERSION 3.20)

project(trevrpc-cpp VERSION 0.2.2 LANGUAGES CXX)

include(CMakePackageConfigHelpers)
include(GNUInstallDirs)
include(CheckCXXSourceCompiles)

find_package(Threads REQUIRED)

# Export compile_commands.json for clangd / editor integration. Honor a
# user-provided value but default to ON.
if(NOT DEFINED CMAKE_EXPORT_COMPILE_COMMANDS)
  set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

check_cxx_source_compiles("
#include <coroutine>
#if !defined(__cpp_impl_coroutine) || __cpp_impl_coroutine < 201902L
#error C++20 coroutine support is required
#endif
int main() { return 0; }
" TREVRPC_CPP_HAS_COROUTINES)
if(NOT TREVRPC_CPP_HAS_COROUTINES)
  message(FATAL_ERROR "trevrpc-cpp requires C++20 <coroutine> support")
endif()

if(CMAKE_SOURCE_DIR STREQUAL CMAKE_CURRENT_SOURCE_DIR)
  set(trevrpc_cpp_default_codegen ON)
else()
  set(trevrpc_cpp_default_codegen OFF)
endif()

option(TREVRPC_CPP_BUILD_CODEGEN "Build protoc-gen-trevrpc-cpp" "${trevrpc_cpp_default_codegen}")
option(TREVRPC_CPP_BUILD_TESTS "Build trevrpc-cpp tests" ON)
option(TREVRPC_CPP_BUILD_EXAMPLES "Build trevrpc-cpp examples" "${TREVRPC_CPP_BUILD_TESTS}")
option(TREVRPC_CPP_BUILD_BENCHMARKS "Build the trevrpc-cpp benchmark peer" OFF)
option(TREVRPC_CPP_ENABLE_SANITIZERS "Build with address and undefined behavior sanitizers" OFF)
set(TREVRPC_CPP_INSTALL_CMAKEDIR "${CMAKE_INSTALL_LIBDIR}/cmake/trevrpc-cpp" CACHE PATH
    "Install directory for trevrpc-cpp CMake package files")
set(TREVRPC_CPP_TREVRPC_C_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../trevrpc-c" CACHE PATH
    "Path to the trevrpc-c source tree; empty uses an installed package")

function(trevrpc_cpp_warnings target)
  if(CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(${target} PRIVATE -Wall -Wextra -Wpedantic -Werror)
  endif()
endfunction()

function(trevrpc_cpp_sanitizers target)
  if(TREVRPC_CPP_ENABLE_SANITIZERS AND CMAKE_CXX_COMPILER_ID MATCHES "Clang|GNU")
    target_compile_options(${target} PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer)
    target_link_options(${target} PRIVATE -fsanitize=address,undefined)
  endif()
endfunction()

if(TARGET trevrpc::trevrpc)
  set(trevrpc_cpp_c_target trevrpc::trevrpc)
elseif(TARGET trevrpc)
  set(trevrpc_cpp_c_target trevrpc)
elseif(TREVRPC_CPP_TREVRPC_C_ROOT)
  if(NOT EXISTS "${TREVRPC_CPP_TREVRPC_C_ROOT}/CMakeLists.txt")
    message(FATAL_ERROR "TREVRPC_CPP_TREVRPC_C_ROOT does not point to trevrpc-c")
  endif()
  set(TREVRPC_BUILD_TESTS OFF)
  set(TREVRPC_BUILD_BENCHMARKS OFF)
  set(TREVRPC_BUILD_EXAMPLES OFF)
  set(TREVRPC_BUILD_CODEGEN OFF)
  set(TREVRPC_BUILD_RUNTIME ON)
  add_subdirectory("${TREVRPC_CPP_TREVRPC_C_ROOT}"
                   "${CMAKE_CURRENT_BINARY_DIR}/trevrpc-c")
  set(trevrpc_cpp_c_target trevrpc)
else()
  find_package(trevrpc 0.1 CONFIG REQUIRED)
  if(NOT TREVRPC_C_ABI_VERSION EQUAL 6)
    message(FATAL_ERROR "trevrpc-cpp requires TrevRPC C ABI 6")
  endif()
  set(trevrpc_cpp_c_target trevrpc::trevrpc)
endif()

add_library(trevrpc_cpp STATIC
  src/trevrpc.cpp
  src/async.cpp
  src/callbacks.cpp
  src/detail/abi6_bridge.cpp
  src/detail/async_core.cpp
  src/detail/lifecycle.cpp)
add_library(trevrpc::cpp ALIAS trevrpc_cpp)
target_include_directories(trevrpc_cpp
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
target_compile_features(trevrpc_cpp PUBLIC cxx_std_20)
target_link_libraries(trevrpc_cpp
  PUBLIC
    $<BUILD_INTERFACE:${trevrpc_cpp_c_target}>
    $<INSTALL_INTERFACE:trevrpc::trevrpc>
    Threads::Threads)
set_target_properties(trevrpc_cpp PROPERTIES EXPORT_NAME cpp)
trevrpc_cpp_warnings(trevrpc_cpp)
trevrpc_cpp_sanitizers(trevrpc_cpp)

if(TREVRPC_CPP_BUILD_CODEGEN OR TREVRPC_CPP_BUILD_TESTS OR TREVRPC_CPP_BUILD_EXAMPLES OR
   TREVRPC_CPP_BUILD_BENCHMARKS)
  find_package(Protobuf CONFIG REQUIRED)
  include(cmake/trevrpc-cpp-generate.cmake)
endif()

if(TREVRPC_CPP_BUILD_CODEGEN OR TREVRPC_CPP_BUILD_TESTS OR TREVRPC_CPP_BUILD_EXAMPLES OR
   TREVRPC_CPP_BUILD_BENCHMARKS)
  add_executable(protoc-gen-trevrpc-cpp
    tools/protoc-gen-trevrpc-cpp/main.cpp
    tools/protoc-gen-trevrpc-cpp/generator.cpp)
  target_link_libraries(protoc-gen-trevrpc-cpp PRIVATE protobuf::libprotoc protobuf::libprotobuf)
  target_include_directories(protoc-gen-trevrpc-cpp PRIVATE tools/protoc-gen-trevrpc-cpp)
  trevrpc_cpp_warnings(protoc-gen-trevrpc-cpp)
  trevrpc_cpp_sanitizers(protoc-gen-trevrpc-cpp)
endif()

install(TARGETS trevrpc_cpp
  EXPORT trevrpcCppTargets
  ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
  LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
  RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
if(TARGET protoc-gen-trevrpc-cpp)
  install(TARGETS protoc-gen-trevrpc-cpp RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
endif()
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
install(EXPORT trevrpcCppTargets
  NAMESPACE trevrpc::
  DESTINATION "${TREVRPC_CPP_INSTALL_CMAKEDIR}")

configure_package_config_file(
  cmake/trevrpc-cppConfig.cmake.in
  "${CMAKE_CURRENT_BINARY_DIR}/trevrpc-cppConfig.cmake"
  INSTALL_DESTINATION "${TREVRPC_CPP_INSTALL_CMAKEDIR}"
  PATH_VARS CMAKE_INSTALL_BINDIR)
write_basic_package_version_file(
  "${CMAKE_CURRENT_BINARY_DIR}/trevrpc-cppConfigVersion.cmake"
  VERSION ${PROJECT_VERSION}
  COMPATIBILITY SameMajorVersion)
install(FILES
  "${CMAKE_CURRENT_BINARY_DIR}/trevrpc-cppConfig.cmake"
  "${CMAKE_CURRENT_BINARY_DIR}/trevrpc-cppConfigVersion.cmake"
  cmake/trevrpc-cpp-generate.cmake
  DESTINATION "${TREVRPC_CPP_INSTALL_CMAKEDIR}")

if(TREVRPC_CPP_BUILD_TESTS)
  enable_testing()
  add_subdirectory(tests)
endif()

if(TREVRPC_CPP_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()

if(TREVRPC_CPP_BUILD_BENCHMARKS)
  add_subdirectory(bench)
endif()
