#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#define REQUIRE(condition)                                                     \
  do {                                                                         \
    if (!(condition)) {                                                        \
      abort();                                                                 \
    }                                                                          \
  } while (0)

static size_t malloc_calls;
static size_t calloc_calls;
static size_t fail_malloc_call;
static size_t fail_calloc_call;

static void *test_malloc(size_t size) {
  malloc_calls++;
  if (malloc_calls == fail_malloc_call) {
    return NULL;
  }
  return malloc(size);
}

static void *test_calloc(size_t count, size_t size) {
  calloc_calls++;
  if (calloc_calls == fail_calloc_call) {
    return NULL;
  }
  return calloc(count, size);
}

#define malloc test_malloc
#define calloc test_calloc
#include "../protocol.c"
#undef calloc
#undef malloc

int cf_dispatch_operation(const cf_command *command,
                          cf_state_dispatch_fn state_dispatch, cf_json *payload,
                          cf_error *error) {
  (void)command;
  (void)state_dispatch;
  (void)payload;
  (void)error;
  return -1;
}

static int parse_with_failures(char *line, size_t fail_malloc,
                               size_t fail_calloc) {
  malloc_calls = 0;
  calloc_calls = 0;
  fail_malloc_call = fail_malloc;
  fail_calloc_call = fail_calloc;
  cf_command command;
  int stop = 0;
  return cf_parse_command(line, strlen(line), &command, &stop);
}

int main(void) {
  char frame_list_allocation[] =
      "RUN\t1\talloc.frames\tstate.server_stream\t2\t0801\t0801";
  REQUIRE(parse_with_failures(frame_list_allocation, 0, 1) == -2);

  char frame_body_allocation[] =
      "RUN\t1\talloc.frame_body\tstate.server_stream\t2\t0801\t0801";
  REQUIRE(parse_with_failures(frame_body_allocation, 2, 0) == -2);

  char metadata_allocation[] =
      "RUN\t1\talloc.metadata\tcodec.encode\trpc_response\t0\t\t\t1\t61\t62";
  REQUIRE(parse_with_failures(metadata_allocation, 0, 1) != 0);

  char metadata_value_allocation[] = "RUN\t1\talloc.metadata_value\tcodec."
                                     "encode\trpc_response\t0\t\t\t1\t61\t62";
  REQUIRE(parse_with_failures(metadata_value_allocation, 2, 0) != 0);
  return 0;
}
