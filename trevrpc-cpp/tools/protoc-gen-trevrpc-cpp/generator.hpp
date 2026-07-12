#pragma once

#include <cstdint>
#include <string>
#include <string_view>

#include <google/protobuf/compiler/code_generator.h>

namespace trevrpc::compiler {

struct GeneratorOptions {
  std::string runtime_include = "trevrpc/trevrpc.hpp";
  std::string header_suffix = ".trevrpc.hpp";
  std::string source_suffix = ".trevrpc.cpp";
};

[[nodiscard]] bool ParseGeneratorOptions(std::string_view parameter, GeneratorOptions* options,
                                         std::string* error);

class TrevRpcCppGenerator final : public google::protobuf::compiler::CodeGenerator {
public:
  [[nodiscard]] bool Generate(const google::protobuf::FileDescriptor* file,
                              const std::string& parameter,
                              google::protobuf::compiler::GeneratorContext* generator_context,
                              std::string* error) const override;

  [[nodiscard]] std::uint64_t GetSupportedFeatures() const override;
};

} // namespace trevrpc::compiler
