#include "generator.hpp"

#include <cassert>
#include <string>

int main() {
  trevrpc::compiler::GeneratorOptions options;
  std::string error;
  assert(trevrpc::compiler::ParseGeneratorOptions(
      "runtime_include=custom/runtime.hpp,header_suffix=.rpc.hpp,source_suffix=.rpc.cpp", &options,
      &error));
  assert(options.runtime_include == "custom/runtime.hpp");
  assert(options.header_suffix == ".rpc.hpp");
  assert(options.source_suffix == ".rpc.cpp");

  assert(!trevrpc::compiler::ParseGeneratorOptions("unknown=value", &options, &error));
  assert(error.find("unknown trevrpc-cpp option") != std::string::npos);
  assert(!trevrpc::compiler::ParseGeneratorOptions("runtime_include=<unsafe>", &options, &error));
  assert(error.find("invalid runtime_include") != std::string::npos);
  return 0;
}
