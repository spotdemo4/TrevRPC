#define _GNU_SOURCE
#define QUIC_API_ENABLE_PREVIEW_FEATURES

#include <dlfcn.h>
#include <errno.h>
#include <limits.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <msquic.h>

#ifndef EXPECT_DESCRIPTOR
#error "EXPECT_DESCRIPTOR must be defined"
#endif

#ifndef REQUESTED_MASK
#error "REQUESTED_MASK must be defined"
#endif

#ifndef EXPECTED_PROVIDER_PATH
#error "EXPECTED_PROVIDER_PATH must be defined"
#endif

#define TREV_QUIC_PARAM_GLOBAL_RESET_STREAM_AT_DIALECTS 0x0100000Fu
#define TREV_QUIC_PARAM_CONFIGURATION_RESET_STREAM_AT_DIALECT_MASK 0x03000004u
#define TREV_RESET_STREAM_AT_DIALECT_DRAFT_07 (1u << 0)
#define TREV_RESET_STREAM_AT_DIALECT_DRAFT_10 (1u << 1)
#define TREV_RESET_STREAM_AT_SUPPORTED_DIALECT_MASK                            \
  (TREV_RESET_STREAM_AT_DIALECT_DRAFT_07 |                                     \
   TREV_RESET_STREAM_AT_DIALECT_DRAFT_10)

struct trev_reset_stream_at_dialects {
  uint32_t version;
  uint32_t supported_dialect_mask;
  uint64_t draft07_transport_parameter_id;
  uint64_t draft10_transport_parameter_id;
  uint64_t frame_type;
};

_Static_assert(sizeof(struct trev_reset_stream_at_dialects) == 32,
               "RESET_STREAM_AT descriptor size changed");
_Static_assert(offsetof(struct trev_reset_stream_at_dialects, version) == 0,
               "RESET_STREAM_AT version offset changed");
_Static_assert(offsetof(struct trev_reset_stream_at_dialects,
                        supported_dialect_mask) == 4,
               "RESET_STREAM_AT mask offset changed");
_Static_assert(offsetof(struct trev_reset_stream_at_dialects,
                        draft07_transport_parameter_id) == 8,
               "RESET_STREAM_AT draft-07 offset changed");
_Static_assert(offsetof(struct trev_reset_stream_at_dialects,
                        draft10_transport_parameter_id) == 16,
               "RESET_STREAM_AT draft-10 offset changed");
_Static_assert(offsetof(struct trev_reset_stream_at_dialects, frame_type) == 24,
               "RESET_STREAM_AT frame offset changed");

static void fail_message(const char *message) {
  fprintf(stderr, "%s\n", message);
  exit(EXIT_FAILURE);
}

static void require_status(const char *operation, QUIC_STATUS actual,
                           QUIC_STATUS expected) {
  if (actual != expected) {
    fprintf(stderr, "%s returned 0x%llx, expected 0x%llx\n", operation,
            (unsigned long long)(uint64_t)actual,
            (unsigned long long)(uint64_t)expected);
    exit(EXIT_FAILURE);
  }
}

static void attest_loaded_provider(void) {
  Dl_info info = {0};
  char resolved[PATH_MAX];
  const size_t provider_length = strlen(EXPECTED_PROVIDER_PATH);

  if (strstr(EXPECTED_PROVIDER_PATH, "libmsquic-2.6.0") == NULL) {
    fail_message("expected provider is not the pinned libmsquic 2.6.0");
  }
  if (dladdr((const void *)(uintptr_t)&MsQuicOpenVersion, &info) == 0 ||
      info.dli_fname == NULL) {
    fail_message("unable to identify the loaded MsQuic provider");
  }
  if (realpath(info.dli_fname, resolved) == NULL) {
    fprintf(stderr, "unable to resolve loaded MsQuic provider %s: %s\n",
            info.dli_fname, strerror(errno));
    exit(EXIT_FAILURE);
  }
  if (strncmp(resolved, EXPECTED_PROVIDER_PATH, provider_length) != 0 ||
      resolved[provider_length] != '/') {
    fprintf(stderr,
            "loaded MsQuic provider %s is outside expected store path %s\n",
            resolved, EXPECTED_PROVIDER_PATH);
    exit(EXIT_FAILURE);
  }
  if (strstr(resolved, "/lib/libmsquic.so") == NULL) {
    fprintf(stderr, "unexpected MsQuic shared library path: %s\n", resolved);
    exit(EXIT_FAILURE);
  }
}

static void attest_global_descriptor(const QUIC_API_TABLE *api) {
  struct trev_reset_stream_at_dialects descriptor = {0};
  uint32_t length = 0;
  QUIC_STATUS status = api->GetParam(
      NULL, TREV_QUIC_PARAM_GLOBAL_RESET_STREAM_AT_DIALECTS, &length, NULL);

#if EXPECT_DESCRIPTOR
  require_status("zero-length descriptor query", status,
                 QUIC_STATUS_BUFFER_TOO_SMALL);
  if (length != sizeof(descriptor)) {
    fail_message("descriptor query returned the wrong required length");
  }

  length = sizeof(descriptor);
  status = api->GetParam(NULL, TREV_QUIC_PARAM_GLOBAL_RESET_STREAM_AT_DIALECTS,
                         &length, NULL);
  require_status("null descriptor query", status,
                 QUIC_STATUS_INVALID_PARAMETER);
  if (length != sizeof(descriptor)) {
    fail_message("null descriptor query changed the required length");
  }

  length = sizeof(descriptor);
  status = api->GetParam(NULL, TREV_QUIC_PARAM_GLOBAL_RESET_STREAM_AT_DIALECTS,
                         &length, &descriptor);
  require_status("descriptor query", status, QUIC_STATUS_SUCCESS);
  if (length != sizeof(descriptor) || descriptor.version != 1u ||
      descriptor.supported_dialect_mask !=
          TREV_RESET_STREAM_AT_SUPPORTED_DIALECT_MASK ||
      descriptor.draft07_transport_parameter_id != 0x17f7586d2cb571ULL ||
      descriptor.draft10_transport_parameter_id != 0x1dULL ||
      descriptor.frame_type != 0x24ULL) {
    fail_message("provider returned a mismatched RESET_STREAM_AT descriptor");
  }
#else
  require_status("stock descriptor query", status,
                 QUIC_STATUS_INVALID_PARAMETER);
#endif

  status = api->SetParam(NULL, TREV_QUIC_PARAM_GLOBAL_RESET_STREAM_AT_DIALECTS,
                         sizeof(descriptor), &descriptor);
  require_status("global descriptor mutation", status,
                 QUIC_STATUS_INVALID_PARAMETER);
}

static void attest_configuration_mask(const QUIC_API_TABLE *api) {
  static uint8_t alpn_bytes[] = "trevrpc-provider-check";
  QUIC_BUFFER alpn = {
      (uint32_t)(sizeof(alpn_bytes) - 1u),
      alpn_bytes,
  };
  HQUIC registration = NULL;
  HQUIC configuration = NULL;
  uint32_t mask = REQUESTED_MASK;
  uint32_t length = sizeof(mask);
  QUIC_STATUS status = api->RegistrationOpen(NULL, &registration);

  require_status("registration open", status, QUIC_STATUS_SUCCESS);
  status = api->ConfigurationOpen(registration, &alpn, 1, NULL, 0, NULL,
                                  &configuration);
  require_status("configuration open", status, QUIC_STATUS_SUCCESS);

  status = api->SetParam(
      configuration, TREV_QUIC_PARAM_CONFIGURATION_RESET_STREAM_AT_DIALECT_MASK,
      sizeof(mask), &mask);
#if EXPECT_DESCRIPTOR
  require_status("configuration mask set", status, QUIC_STATUS_SUCCESS);

  mask = 0;
  length = sizeof(mask);
  status = api->GetParam(
      configuration, TREV_QUIC_PARAM_CONFIGURATION_RESET_STREAM_AT_DIALECT_MASK,
      &length, &mask);
  require_status("configuration mask get", status, QUIC_STATUS_SUCCESS);
  if (length != sizeof(mask) || mask != REQUESTED_MASK) {
    fail_message("configuration mask did not round-trip exactly");
  }

  mask = 0x4u;
  status = api->SetParam(
      configuration, TREV_QUIC_PARAM_CONFIGURATION_RESET_STREAM_AT_DIALECT_MASK,
      sizeof(mask), &mask);
  require_status("unknown configuration mask bit", status,
                 QUIC_STATUS_INVALID_PARAMETER);

  mask = 0;
  length = sizeof(mask);
  status = api->GetParam(
      configuration, TREV_QUIC_PARAM_CONFIGURATION_RESET_STREAM_AT_DIALECT_MASK,
      &length, &mask);
  require_status("configuration mask get after rejection", status,
                 QUIC_STATUS_SUCCESS);
  if (length != sizeof(mask) || mask != REQUESTED_MASK) {
    fail_message("rejected mask changed the configured dialects");
  }
#else
  require_status("stock configuration mask set", status,
                 QUIC_STATUS_INVALID_PARAMETER);

  mask = 0;
  length = sizeof(mask);
  status = api->GetParam(
      configuration, TREV_QUIC_PARAM_CONFIGURATION_RESET_STREAM_AT_DIALECT_MASK,
      &length, &mask);
  require_status("stock configuration mask get", status,
                 QUIC_STATUS_INVALID_PARAMETER);
#endif

  api->ConfigurationClose(configuration);
  api->RegistrationClose(registration);
}

int main(void) {
  const QUIC_API_TABLE *api = NULL;
  QUIC_STATUS status = MsQuicOpen2(&api);

  require_status("MsQuicOpen2", status, QUIC_STATUS_SUCCESS);
  if (api == NULL) {
    fail_message("MsQuicOpen2 returned a null API table");
  }

  attest_loaded_provider();
  attest_global_descriptor(api);
  attest_configuration_mask(api);

  MsQuicClose(api);
  return EXIT_SUCCESS;
}
