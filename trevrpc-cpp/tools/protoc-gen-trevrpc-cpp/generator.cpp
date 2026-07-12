#include "generator.hpp"

#include <algorithm>
#include <cstring>
#include <exception>
#include <iterator>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>

#include <google/protobuf/compiler/cpp/names.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/zero_copy_stream.h>

namespace trevrpc::compiler {
namespace {

using google::protobuf::Descriptor;
using google::protobuf::FileDescriptor;
using google::protobuf::MethodDescriptor;
using google::protobuf::ServiceDescriptor;
using google::protobuf::compiler::GeneratorContext;

constexpr std::string_view kCppKeywords[] = {
    "alignas",       "alignof",
    "and",           "and_eq",
    "asm",           "atomic_cancel",
    "atomic_commit", "atomic_noexcept",
    "auto",          "bitand",
    "bitor",         "bool",
    "break",         "case",
    "catch",         "char",
    "char8_t",       "char16_t",
    "char32_t",      "class",
    "co_await",      "co_return",
    "co_yield",      "compl",
    "concept",       "const",
    "consteval",     "constexpr",
    "constinit",     "const_cast",
    "continue",      "contract_assert",
    "decltype",      "default",
    "delete",        "do",
    "double",        "dynamic_cast",
    "else",          "enum",
    "explicit",      "export",
    "extern",        "false",
    "final",         "float",
    "for",           "friend",
    "goto",          "if",
    "import",        "inline",
    "int",           "long",
    "module",        "mutable",
    "namespace",     "new",
    "noexcept",      "not",
    "not_eq",        "nullptr",
    "operator",      "or",
    "or_eq",         "override",
    "private",       "protected",
    "public",        "reflexpr",
    "register",      "reinterpret_cast",
    "requires",      "return",
    "short",         "signed",
    "sizeof",        "static",
    "static_assert", "static_cast",
    "struct",        "switch",
    "synchronized",  "template",
    "this",          "thread_local",
    "throw",         "true",
    "try",           "typedef",
    "typeid",        "typename",
    "union",         "unsigned",
    "using",         "virtual",
    "void",          "volatile",
    "wchar_t",       "while",
    "xor",           "xor_eq",
};

enum class RpcShape {
  kUnary,
  kServerStreaming,
  kClientStreaming,
  kBidirectionalStreaming,
};

[[nodiscard]] std::string CppIdentifier(std::string_view name) {
  if (std::find(std::begin(kCppKeywords), std::end(kCppKeywords), name) != std::end(kCppKeywords)) {
    return std::string(name) + '_';
  }
  return std::string(name);
}

[[nodiscard]] std::string ServiceTypeName(const ServiceDescriptor& service) {
  return CppIdentifier(service.name()) + "Service";
}

[[nodiscard]] std::string ClientTypeName(const ServiceDescriptor& service) {
  return CppIdentifier(service.name()) + "Client";
}

[[nodiscard]] std::string MethodName(const ServiceDescriptor& service,
                                     const MethodDescriptor& method, bool client_method) {
  std::string name = CppIdentifier(method.name());
  const std::string& enclosing_name =
      client_method ? ClientTypeName(service) : ServiceTypeName(service);
  if (name == enclosing_name) {
    name.push_back('_');
  }
  return name;
}

[[nodiscard]] RpcShape Shape(const MethodDescriptor& method) {
  if (method.client_streaming() && method.server_streaming()) {
    return RpcShape::kBidirectionalStreaming;
  }
  if (method.client_streaming()) {
    return RpcShape::kClientStreaming;
  }
  if (method.server_streaming()) {
    return RpcShape::kServerStreaming;
  }
  return RpcShape::kUnary;
}

[[nodiscard]] std::string MessageType(const Descriptor& descriptor) {
  return google::protobuf::compiler::cpp::QualifiedClassName(&descriptor);
}

[[nodiscard]] std::string ProtoStem(std::string_view file_name) {
  return google::protobuf::compiler::cpp::StripProto(file_name);
}

[[nodiscard]] std::string Quoted(std::string_view value) {
  std::string result;
  result.reserve(value.size() + 2);
  result.push_back('"');
  for (const unsigned char character : value) {
    switch (character) {
    case '\\':
      result += "\\\\";
      break;
    case '"':
      result += "\\\"";
      break;
    case '\n':
      result += "\\n";
      break;
    case '\r':
      result += "\\r";
      break;
    case '\t':
      result += "\\t";
      break;
    default:
      result.push_back(static_cast<char>(character));
      break;
    }
  }
  result.push_back('"');
  return result;
}

[[nodiscard]] std::string ServicePath(const FileDescriptor& file,
                                      const ServiceDescriptor& service) {
  if (file.package().empty()) {
    return std::string(service.name());
  }
  std::string path(file.package());
  path.push_back('.');
  path.append(service.name());
  return path;
}

[[nodiscard]] std::string NamespaceName(const FileDescriptor& file) {
  std::string name = google::protobuf::compiler::cpp::Namespace(&file);
  if (name.starts_with("::")) {
    name.erase(0, 2);
  }
  return name;
}

void OpenNamespace(std::ostringstream& output, const FileDescriptor& file) {
  const std::string name = NamespaceName(file);
  if (!name.empty()) {
    output << "namespace " << name << " {\n\n";
  }
}

void CloseNamespace(std::ostringstream& output, const FileDescriptor& file) {
  const std::string name = NamespaceName(file);
  if (!name.empty()) {
    output << "} // namespace " << name << "\n";
  }
}

void GenerateServiceInterface(std::ostringstream& output, const ServiceDescriptor& service) {
  const std::string service_type = ServiceTypeName(service);
  output << "class " << service_type << " {\n"
         << "  public:\n"
         << "    virtual ~" << service_type << "() = default;\n\n";

  for (int index = 0; index < service.method_count(); ++index) {
    const MethodDescriptor& method = *service.method(index);
    const std::string input = MessageType(*method.input_type());
    const std::string response = MessageType(*method.output_type());
    const std::string method_name = MethodName(service, method, false);

    switch (Shape(method)) {
    case RpcShape::kUnary:
      output << "    [[nodiscard]] virtual ::trevrpc::Result<" << response << "> " << method_name
             << "(const ::trevrpc::CallContext& context, const " << input << "& request) = 0;\n";
      break;
    case RpcShape::kServerStreaming:
      output << "    [[nodiscard]] virtual ::trevrpc::Status " << method_name
             << "(const ::trevrpc::CallContext& context, const " << input
             << "& request, ::trevrpc::ServerWriter<" << response << ">& writer) = 0;\n";
      break;
    case RpcShape::kClientStreaming:
      output << "    [[nodiscard]] virtual ::trevrpc::Result<" << response << "> " << method_name
             << "(const ::trevrpc::CallContext& context, ::trevrpc::ServerReader<" << input
             << ">& reader) = 0;\n";
      break;
    case RpcShape::kBidirectionalStreaming:
      output << "    [[nodiscard]] virtual ::trevrpc::Status " << method_name
             << "(const ::trevrpc::CallContext& context, "
                "::trevrpc::ServerReaderWriter<"
             << input << ", " << response << ">& stream) = 0;\n";
      break;
    }
  }

  output << "};\n\n";
}

void GenerateClientDeclaration(std::ostringstream& output, const ServiceDescriptor& service) {
  const std::string client_type = ClientTypeName(service);
  output << "class " << client_type << " final {\n"
         << "  public:\n"
         << "    explicit " << client_type << "(std::shared_ptr<::trevrpc::Channel> channel);\n\n";

  for (int index = 0; index < service.method_count(); ++index) {
    const MethodDescriptor& method = *service.method(index);
    const std::string input = MessageType(*method.input_type());
    const std::string response = MessageType(*method.output_type());
    const std::string method_name = MethodName(service, method, true);

    switch (Shape(method)) {
    case RpcShape::kUnary:
      output << "    [[nodiscard]] ::trevrpc::Result<::trevrpc::UnaryResponse<" << response << ">> "
             << method_name << "(const " << input
             << "& request, const ::trevrpc::CallOptions& options = {}) const;\n";
      break;
    case RpcShape::kServerStreaming:
      output << "    [[nodiscard]] "
                "::trevrpc::Result<::trevrpc::ServerStreamingCall<"
             << response << ">> " << method_name << "(const " << input
             << "& request, const ::trevrpc::CallOptions& options = {}) const;\n";
      break;
    case RpcShape::kClientStreaming:
      output << "    [[nodiscard]] "
                "::trevrpc::Result<::trevrpc::ClientStreamingCall<"
             << input << ", " << response << ">> " << method_name
             << "(const ::trevrpc::CallOptions& options = {}) const;\n";
      break;
    case RpcShape::kBidirectionalStreaming:
      output << "    [[nodiscard]] "
                "::trevrpc::Result<::trevrpc::BidirectionalStreamingCall<"
             << input << ", " << response << ">> " << method_name
             << "(const ::trevrpc::CallOptions& options = {}) const;\n";
      break;
    }
  }

  output << "\n  private:\n"
         << "    std::shared_ptr<::trevrpc::Channel> channel_;\n"
         << "};\n\n";
}

void GenerateRegistrationDeclaration(std::ostringstream& output, const ServiceDescriptor& service) {
  output << "[[nodiscard]] ::trevrpc::Result<void> Register" << CppIdentifier(service.name())
         << "(::trevrpc::Server& server, std::shared_ptr<" << ServiceTypeName(service)
         << "> service);\n\n";
}

[[nodiscard]] std::string GenerateHeader(const FileDescriptor& file,
                                         const GeneratorOptions& options) {
  std::ostringstream output;
  output << "// Code generated by protoc-gen-trevrpc-cpp. DO NOT EDIT.\n"
         << "#pragma once\n\n"
         << "#include <memory>\n\n"
         << "#include <" << options.runtime_include << ">\n"
         << "#include " << Quoted(ProtoStem(file.name()) + ".pb.h") << "\n\n";

  OpenNamespace(output, file);
  for (int index = 0; index < file.service_count(); ++index) {
    const ServiceDescriptor& service = *file.service(index);
    GenerateServiceInterface(output, service);
    GenerateClientDeclaration(output, service);
    GenerateRegistrationDeclaration(output, service);
  }
  CloseNamespace(output, file);
  return output.str();
}

void GenerateClientDefinition(std::ostringstream& output, const FileDescriptor& file,
                              const ServiceDescriptor& service) {
  const std::string client_type = ClientTypeName(service);
  output << client_type << "::" << client_type << "(std::shared_ptr<::trevrpc::Channel> channel)\n"
         << "    : channel_(std::move(channel)) {}\n\n";

  const std::string service_path = Quoted(ServicePath(file, service));
  for (int index = 0; index < service.method_count(); ++index) {
    const MethodDescriptor& method = *service.method(index);
    const std::string input = MessageType(*method.input_type());
    const std::string response = MessageType(*method.output_type());
    const std::string method_name = MethodName(service, method, true);
    const std::string method_path = Quoted(method.name());

    switch (Shape(method)) {
    case RpcShape::kUnary:
      output << "::trevrpc::Result<::trevrpc::UnaryResponse<" << response << ">> " << client_type
             << "::" << method_name << "(const " << input
             << "& request, const ::trevrpc::CallOptions& options) const {\n"
             << "    return ::trevrpc::unary<" << input << ", " << response << ">(*channel_, "
             << service_path << ", " << method_path << ", request, options);\n"
             << "}\n\n";
      break;
    case RpcShape::kServerStreaming:
      output << "::trevrpc::Result<::trevrpc::ServerStreamingCall<" << response << ">> "
             << client_type << "::" << method_name << "(const " << input
             << "& request, const ::trevrpc::CallOptions& options) const {\n"
             << "    return ::trevrpc::server_streaming<" << input << ", " << response
             << ">(*channel_, " << service_path << ", " << method_path << ", request, options);\n"
             << "}\n\n";
      break;
    case RpcShape::kClientStreaming:
      output << "::trevrpc::Result<::trevrpc::ClientStreamingCall<" << input << ", " << response
             << ">> " << client_type << "::" << method_name
             << "(const ::trevrpc::CallOptions& options) const {\n"
             << "    return ::trevrpc::client_streaming<" << input << ", " << response
             << ">(*channel_, " << service_path << ", " << method_path << ", options);\n"
             << "}\n\n";
      break;
    case RpcShape::kBidirectionalStreaming:
      output << "::trevrpc::Result<::trevrpc::BidirectionalStreamingCall<" << input << ", "
             << response << ">> " << client_type << "::" << method_name
             << "(const ::trevrpc::CallOptions& options) const {\n"
             << "    return ::trevrpc::bidirectional_streaming<" << input << ", " << response
             << ">(*channel_, " << service_path << ", " << method_path << ", options);\n"
             << "}\n\n";
      break;
    }
  }
}

void GenerateRegistrationDefinition(std::ostringstream& output, const FileDescriptor& file,
                                    const ServiceDescriptor& service) {
  const std::string service_type = ServiceTypeName(service);
  output << "::trevrpc::Result<void> Register" << CppIdentifier(service.name())
         << "(::trevrpc::Server& server, std::shared_ptr<" << service_type << "> service) {\n"
         << "    if (!service) {\n"
         << "        return ::trevrpc::Error::runtime(-22, \"service must not be null\");\n"
         << "    }\n";

  const std::string service_path = Quoted(ServicePath(file, service));
  for (int index = 0; index < service.method_count(); ++index) {
    const MethodDescriptor& method = *service.method(index);
    const std::string input = MessageType(*method.input_type());
    const std::string response = MessageType(*method.output_type());
    const std::string method_name = MethodName(service, method, false);
    const std::string method_path = Quoted(method.name());

    switch (Shape(method)) {
    case RpcShape::kUnary:
      output << "    auto registered_" << index << " = server.register_unary<" << input << ", "
             << response << ">(" << service_path << ", " << method_path
             << ", [service](const ::trevrpc::CallContext& context, const " << input
             << "& request) {\n"
             << "        return service->" << method_name << "(context, request);\n"
             << "    });\n";
      break;
    case RpcShape::kServerStreaming:
      output << "    auto registered_" << index << " = server.register_server_streaming<" << input
             << ", " << response << ">(" << service_path << ", " << method_path
             << ", [service](const ::trevrpc::CallContext& context, const " << input
             << "& request, ::trevrpc::ServerWriter<" << response << ">& writer) {\n"
             << "        return service->" << method_name << "(context, request, writer);\n"
             << "    });\n";
      break;
    case RpcShape::kClientStreaming:
      output << "    auto registered_" << index << " = server.register_client_streaming<" << input
             << ", " << response << ">(" << service_path << ", " << method_path
             << ", [service](const ::trevrpc::CallContext& context, "
                "::trevrpc::ServerReader<"
             << input << ">& reader) {\n"
             << "        return service->" << method_name << "(context, reader);\n"
             << "    });\n";
      break;
    case RpcShape::kBidirectionalStreaming:
      output << "    auto registered_" << index << " = server.register_bidirectional_streaming<"
             << input << ", " << response << ">(" << service_path << ", " << method_path
             << ", [service](const ::trevrpc::CallContext& context, "
                "::trevrpc::ServerReaderWriter<"
             << input << ", " << response << ">& stream) {\n"
             << "        return service->" << method_name << "(context, stream);\n"
             << "    });\n";
      break;
    }
    output << "    if (!registered_" << index << ") {\n"
           << "        return registered_" << index << ".error();\n"
           << "    }\n";
  }
  output << "    return {};\n"
         << "}\n\n";
}

[[nodiscard]] std::string GenerateSource(const FileDescriptor& file,
                                         const GeneratorOptions& options) {
  std::ostringstream output;
  output << "// Code generated by protoc-gen-trevrpc-cpp. DO NOT EDIT.\n\n"
         << "#include " << Quoted(ProtoStem(file.name()) + options.header_suffix) << "\n\n"
         << "#include <utility>\n\n";

  OpenNamespace(output, file);
  for (int index = 0; index < file.service_count(); ++index) {
    const ServiceDescriptor& service = *file.service(index);
    GenerateClientDefinition(output, file, service);
    GenerateRegistrationDefinition(output, file, service);
  }
  CloseNamespace(output, file);
  return output.str();
}

[[nodiscard]] bool WriteFile(GeneratorContext& context, const std::string& file_name,
                             const std::string& content, std::string* error) {
  std::unique_ptr<google::protobuf::io::ZeroCopyOutputStream> stream(context.Open(file_name));
  std::size_t offset = 0;
  while (offset < content.size()) {
    void* buffer = nullptr;
    int buffer_size = 0;
    if (!stream->Next(&buffer, &buffer_size) || buffer_size <= 0) {
      if (error != nullptr) {
        *error = "failed to write generated file " + Quoted(file_name);
      }
      return false;
    }

    const std::size_t available = static_cast<std::size_t>(buffer_size);
    const std::size_t write_size = std::min(available, content.size() - offset);
    std::memcpy(buffer, content.data() + offset, write_size);
    offset += write_size;
    if (write_size < available) {
      stream->BackUp(static_cast<int>(available - write_size));
    }
  }
  return true;
}

[[nodiscard]] bool IsSafeIncludePath(std::string_view value) {
  return !value.empty() && value.find_first_of("<>\"\r\n") == std::string_view::npos;
}

} // namespace

bool ParseGeneratorOptions(std::string_view parameter, GeneratorOptions* options,
                           std::string* error) {
  if (options == nullptr) {
    if (error != nullptr) {
      *error = "generator options output must not be null";
    }
    return false;
  }

  *options = GeneratorOptions{};
  while (!parameter.empty()) {
    const std::size_t comma = parameter.find(',');
    const std::string_view option = parameter.substr(0, comma);
    parameter = comma == std::string_view::npos ? std::string_view{} : parameter.substr(comma + 1);
    if (option.empty()) {
      continue;
    }

    const std::size_t equals = option.find('=');
    if (equals == std::string_view::npos) {
      if (error != nullptr) {
        *error = "invalid trevrpc-cpp option " + Quoted(option) + "; expected key=value";
      }
      return false;
    }

    const std::string_view key = option.substr(0, equals);
    const std::string_view value = option.substr(equals + 1);
    if (key == "runtime_include") {
      if (!IsSafeIncludePath(value)) {
        if (error != nullptr) {
          *error = "invalid runtime_include " + Quoted(value);
        }
        return false;
      }
      options->runtime_include = value;
    } else if (key == "header_suffix") {
      if (value.empty()) {
        if (error != nullptr) {
          *error = "header_suffix must not be empty";
        }
        return false;
      }
      options->header_suffix = value;
    } else if (key == "source_suffix") {
      if (value.empty()) {
        if (error != nullptr) {
          *error = "source_suffix must not be empty";
        }
        return false;
      }
      options->source_suffix = value;
    } else {
      if (error != nullptr) {
        *error = "unknown trevrpc-cpp option " + Quoted(key);
      }
      return false;
    }
  }
  return true;
}

bool TrevRpcCppGenerator::Generate(const FileDescriptor* file, const std::string& parameter,
                                   GeneratorContext* generator_context, std::string* error) const {
  if (file == nullptr || generator_context == nullptr) {
    if (error != nullptr) {
      *error = "generator received a null file or context";
    }
    return false;
  }

  if (file->service_count() == 0) {
    return true;
  }

  GeneratorOptions options;
  if (!ParseGeneratorOptions(parameter, &options, error)) {
    return false;
  }

  try {
    const std::string stem = ProtoStem(file->name());
    const std::string header = GenerateHeader(*file, options);
    const std::string source = GenerateSource(*file, options);
    return WriteFile(*generator_context, stem + options.header_suffix, header, error) &&
           WriteFile(*generator_context, stem + options.source_suffix, source, error);
  } catch (const std::exception& exception) {
    if (error != nullptr) {
      *error = std::string("trevrpc-cpp generation failed: ") + exception.what();
    }
    return false;
  }
}

std::uint64_t TrevRpcCppGenerator::GetSupportedFeatures() const { return FEATURE_PROTO3_OPTIONAL; }

} // namespace trevrpc::compiler
