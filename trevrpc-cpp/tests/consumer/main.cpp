#include "nested/service.trevrpc.hpp"

int main() {
  const trevrpc::Result<int> result(42);
  trevrpc::cpp::consumer::Request request;
  request.set_value("generated");
  return result && result.value() == 42 && request.value() == "generated" ? 0 : 1;
}
