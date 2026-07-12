#include "generator.hpp"

#include <google/protobuf/compiler/plugin.h>

int main(int argc, char* argv[]) {
  const trevrpc::compiler::TrevRpcCppGenerator generator;
  return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
