#include "trevrpc.h"
#include "trevrpc_wire_internal.h"

#include <errno.h> // IWYU pragma: keep
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef TREVRPC_WIRE_GOLDEN_VECTORS
#define TREVRPC_WIRE_GOLDEN_VECTORS "../testdata/wire-golden-vectors.txt"
#endif

#define CHECK_GOTO(condition)                                                                                          \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            result = 1;                                                                                                \
            goto cleanup;                                                                                              \
        }                                                                                                              \
    } while (0)

static size_t frame_body_len(const uint8_t* frame) {
    return ((size_t)frame[0] << 24) | ((size_t)frame[1] << 16) | ((size_t)frame[2] << 8) | (size_t)frame[3];
}

static int bytes_equal(const uint8_t* got, size_t got_len, const uint8_t* want, size_t want_len) {
    return got_len == want_len && (got_len == 0 || memcmp(got, want, got_len) == 0);
}

static int chars_equal(const char* got, size_t got_len, const char* want) {
    size_t want_len = strlen(want);
    return got_len == want_len && (got_len == 0 || memcmp(got, want, got_len) == 0);
}

static int metadata_value_equal(
    const trevrpc_metadata* metadata, const char* key, const uint8_t* value, size_t value_len) {
    size_t key_len = strlen(key);
    for (size_t i = 0; i < metadata->entries_len; i++) {
        const trevrpc_metadata_entry* entry = &metadata->entries[i];
        if (entry->key_len == key_len && memcmp(entry->key, key, key_len) == 0) {
            return bytes_equal(entry->value, entry->value_len, value, value_len);
        }
    }
    return 0;
}

static const char* trim_left(const char* value) {
    while (*value == ' ' || *value == '\t' || *value == '\r' || *value == '\n') {
        value++;
    }
    return value;
}

static void trim_right(char* value) {
    size_t len = strlen(value);
    while (len > 0 &&
           (value[len - 1] == ' ' || value[len - 1] == '\t' || value[len - 1] == '\r' || value[len - 1] == '\n')) {
        value[len - 1] = '\0';
        len--;
    }
}

static int hex_value(char value) {
    if (value >= '0' && value <= '9') {
        return value - '0';
    }
    if (value >= 'a' && value <= 'f') {
        return value - 'a' + 10;
    }
    if (value >= 'A' && value <= 'F') {
        return value - 'A' + 10;
    }
    return -1;
}

static int decode_hex(const char* encoded, uint8_t** out, size_t* out_len) {
    *out = NULL;
    *out_len = 0;
    size_t encoded_len = strlen(encoded);
    if (encoded_len % 2 != 0) {
        return -EINVAL;
    }

    size_t decoded_len = encoded_len / 2;
    uint8_t* decoded = NULL;
    if (decoded_len > 0) {
        decoded = malloc(decoded_len);
        if (decoded == NULL) {
            return -ENOMEM;
        }
    }

    for (size_t i = 0; i < decoded_len; i++) {
        int high = hex_value(encoded[i * 2]);
        int low = hex_value(encoded[i * 2 + 1]);
        if (high < 0 || low < 0) {
            free(decoded);
            return -EINVAL;
        }
        decoded[i] = (uint8_t)((high << 4) | low);
    }

    *out = decoded;
    *out_len = decoded_len;
    return 0;
}

static int assert_golden_vector(const char* name, const uint8_t* actual, size_t actual_len) {
    FILE* file = fopen(TREVRPC_WIRE_GOLDEN_VECTORS, "r");
    if (file == NULL) {
        fprintf(stderr, "failed to open %s\n", TREVRPC_WIRE_GOLDEN_VECTORS);
        return 1;
    }

    int result = 1;
    char line[1024];
    for (size_t line_number = 1; fgets(line, sizeof(line), file) != NULL; line_number++) {
        trim_right(line);
        char* key = (char*)trim_left(line);
        if (key[0] == '\0' || key[0] == '#') {
            continue;
        }

        char* equals = strchr(key, '=');
        if (equals == NULL) {
            fprintf(stderr, "%s:%zu: invalid golden vector line\n", TREVRPC_WIRE_GOLDEN_VECTORS, line_number);
            goto cleanup;
        }
        *equals = '\0';
        trim_right(key);

        char* encoded = (char*)trim_left(equals + 1);
        trim_right(encoded);
        if (strcmp(key, name) != 0) {
            continue;
        }

        uint8_t* expected = NULL;
        size_t expected_len = 0;
        int err = decode_hex(encoded, &expected, &expected_len);
        if (err != 0) {
            fprintf(
                stderr, "%s:%zu: invalid golden vector hex for %s\n", TREVRPC_WIRE_GOLDEN_VECTORS, line_number, name);
            goto cleanup;
        }

        if (!bytes_equal(actual, actual_len, expected, expected_len)) {
            fprintf(stderr, "unexpected golden vector %s\n", name);
            free(expected);
            goto cleanup;
        }

        free(expected);
        result = 0;
        goto cleanup;
    }

    fprintf(stderr, "missing golden vector %s\n", name);

cleanup:
    fclose(file);
    return result;
}

static int assert_golden_message(const char* name, const uint8_t* frame, size_t frame_len) {
    if (frame_len < 4) {
        fprintf(stderr, "%s encoded frame is shorter than the frame header\n", name);
        return 1;
    }

    char vector_name[128];
    int written = snprintf(vector_name, sizeof(vector_name), "%s.body", name);
    if (written < 0 || (size_t)written >= sizeof(vector_name)) {
        return 1;
    }
    if (assert_golden_vector(vector_name, frame + 4, frame_len - 4) != 0) {
        return 1;
    }

    written = snprintf(vector_name, sizeof(vector_name), "%s.frame", name);
    if (written < 0 || (size_t)written >= sizeof(vector_name)) {
        return 1;
    }
    return assert_golden_vector(vector_name, frame, frame_len);
}

static int test_request_round_trip(void) {
    int result = 1;
    uint8_t body[] = {0xde, 0xad, 0xbe, 0xef};
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_request request = {0};

    int err = trevrpc_wire_encode_request("test.EchoService",
        "Echo",
        TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING,
        body,
        sizeof(body),
        NULL,
        0,
        1024,
        &frame,
        &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(frame != NULL);
    CHECK_GOTO(frame_len > 4);
    CHECK_GOTO(frame_body_len(frame) == frame_len - 4);

    err = trevrpc_wire_decode_request(frame + 4, frame_len - 4, &request);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(chars_equal(request.service, request.service_len, "test.EchoService"));
    CHECK_GOTO(chars_equal(request.method, request.method_len, "Echo"));
    CHECK_GOTO(bytes_equal(request.body, request.body_len, body, sizeof(body)));
    CHECK_GOTO(request.kind == TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING);
    CHECK_GOTO(request.version == TREVRPC_WIRE_VERSION);

    result = 0;

cleanup:
    trevrpc_request_reset(&request);
    free(frame);
    return result;
}

static int test_request_rejects_bad_frames(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_request request = {0};
    uint8_t invalid_varint[] = {0x80};

    int err = trevrpc_wire_decode_request(invalid_varint, sizeof(invalid_varint), &request);
    CHECK_GOTO(err == TREVRPC_ERR_INVALID_FRAME);

    err = trevrpc_wire_encode_request(
        "svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 0, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(frame_len > 4);
    frame[frame_len - 1] = 2;
    err = trevrpc_wire_decode_request(frame + 4, frame_len - 4, &request);
    CHECK_GOTO(err == TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION);
    free(frame);
    frame = NULL;

    err = trevrpc_wire_encode_request("svc", "method", 99, NULL, 0, NULL, 0, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    err = trevrpc_wire_decode_request(frame + 4, frame_len - 4, &request);
    CHECK_GOTO(err == TREVRPC_ERR_UNSUPPORTED_RPC_KIND);

    result = 0;

cleanup:
    trevrpc_request_reset(&request);
    free(frame);
    return result;
}

static int test_request_diagnostics_preserve_reliable_kind(void) {
    int result = 1;
    trevrpc_request request = {0};
    trevrpc_wire_request_diagnostic diagnostic = {0};
    const uint8_t unsupported_server_streaming[] = {
        0x0a,
        0x03,
        's',
        'v',
        'c',
        0x12,
        0x01,
        'm',
        0x28,
        0x02,
        0x30,
        0x02,
    };
    const uint8_t unsupported_client_streaming[] = {
        0x0a,
        0x03,
        's',
        'v',
        'c',
        0x12,
        0x01,
        'm',
        0x28,
        0x01,
        0x30,
        0x02,
    };
    const uint8_t unsupported_bidi_streaming[] = {
        0x0a,
        0x03,
        's',
        'v',
        'c',
        0x12,
        0x01,
        'm',
        0x28,
        0x03,
        0x30,
        0x02,
    };
    const uint8_t invalid_metadata_then_streaming[] = {
        0x0a,
        0x03,
        's',
        'v',
        'c',
        0x12,
        0x01,
        'm',
        0x22,
        0x01,
        0x00,
        0x28,
        0x03,
        0x30,
        0x01,
    };
    const uint8_t missing_service_streaming[] = {0x12, 0x01, 'm', 0x28, 0x01, 0x30, 0x01};
    const uint8_t omitted_kind[] = {0x0a, 0x03, 's', 'v', 'c', 0x12, 0x01, 'm', 0x30, 0x01};
    const uint8_t duplicate_kind[] = {
        0x0a,
        0x03,
        's',
        'v',
        'c',
        0x12,
        0x01,
        'm',
        0x28,
        0x01,
        0x28,
        0x03,
        0x30,
        0x01,
    };
    const uint8_t unsupported_kind[] = {
        0x0a,
        0x03,
        's',
        'v',
        'c',
        0x12,
        0x01,
        'm',
        0x28,
        0x63,
        0x30,
        0x01,
    };
    const uint8_t truncated_after_kind[] = {
        0x12,
        0x01,
        'm',
        0x28,
        0x03,
        0x30,
        0x01,
        0x0a,
        0x02,
        's',
    };
    const uint8_t overflowed_kind[] = {
        0x0a,
        0x03,
        's',
        'v',
        'c',
        0x12,
        0x01,
        'm',
        0x28,
        0x83,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x02,
        0x30,
        0x01,
    };
    const uint8_t overflowed_version[] = {
        0x0a,
        0x03,
        's',
        'v',
        'c',
        0x12,
        0x01,
        'm',
        0x28,
        0x03,
        0x30,
        0x81,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x80,
        0x02,
    };
    const struct {
        const uint8_t* data;
        size_t len;
        int error;
        uint32_t response_kind;
    } cases[] = {
        {unsupported_server_streaming,
            sizeof(unsupported_server_streaming),
            TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION,
            TREVRPC_RPC_KIND_SERVER_STREAMING},
        {unsupported_client_streaming,
            sizeof(unsupported_client_streaming),
            TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION,
            TREVRPC_RPC_KIND_CLIENT_STREAMING},
        {unsupported_bidi_streaming,
            sizeof(unsupported_bidi_streaming),
            TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION,
            TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING},
        {invalid_metadata_then_streaming,
            sizeof(invalid_metadata_then_streaming),
            TREVRPC_ERR_INVALID_FRAME,
            TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING},
        {missing_service_streaming, sizeof(missing_service_streaming), 0, TREVRPC_RPC_KIND_CLIENT_STREAMING},
        {omitted_kind, sizeof(omitted_kind), 0, TREVRPC_RPC_KIND_UNARY},
        {duplicate_kind, sizeof(duplicate_kind), 0, TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING},
        {unsupported_kind,
            sizeof(unsupported_kind),
            TREVRPC_ERR_UNSUPPORTED_RPC_KIND,
            TREVRPC_WIRE_REQUEST_KIND_UNKNOWN},
        {truncated_after_kind,
            sizeof(truncated_after_kind),
            TREVRPC_ERR_INVALID_FRAME,
            TREVRPC_WIRE_REQUEST_KIND_UNKNOWN},
        {overflowed_kind, sizeof(overflowed_kind), TREVRPC_ERR_INVALID_FRAME, TREVRPC_WIRE_REQUEST_KIND_UNKNOWN},
        {overflowed_version, sizeof(overflowed_version), TREVRPC_ERR_INVALID_FRAME, TREVRPC_WIRE_REQUEST_KIND_UNKNOWN},
    };

    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        int err = trevrpc_wire_decode_request_diagnostic(cases[i].data, cases[i].len, &request, &diagnostic);
        CHECK_GOTO(err == cases[i].error);
        CHECK_GOTO(diagnostic.response_kind == cases[i].response_kind);
        if (err == 0) {
            CHECK_GOTO(request.kind == cases[i].response_kind);
        } else {
            CHECK_GOTO(request.service == NULL);
            CHECK_GOTO(request.method == NULL);
            CHECK_GOTO(request.metadata.entries == NULL);
            CHECK_GOTO(request.kind == 0);
        }
        trevrpc_request_reset(&request);
    }

    request.kind = TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING;
    int err = trevrpc_wire_decode_request(unsupported_server_streaming, sizeof(unsupported_server_streaming), &request);
    CHECK_GOTO(err == TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION);
    CHECK_GOTO(request.service == NULL);
    CHECK_GOTO(request.method == NULL);
    CHECK_GOTO(request.kind == 0);

    result = 0;

cleanup:
    trevrpc_request_reset(&request);
    return result;
}

static int test_encode_rejects_oversized_frames(void) {
    int result = 1;
    uint8_t body[] = {0x01, 0x02, 0x03, 0x04};
    uint8_t* frame = NULL;
    size_t frame_len = 0;

    int err = trevrpc_wire_encode_request(
        "svc", "method", TREVRPC_RPC_KIND_UNARY, body, sizeof(body), NULL, 0, 1, &frame, &frame_len);
    CHECK_GOTO(err == TREVRPC_ERR_FRAME_TOO_LARGE);
    CHECK_GOTO(frame == NULL);
    CHECK_GOTO(frame_len == 0);

    result = 0;

cleanup:
    free(frame);
    return result;
}

static int test_encode_accepts_exact_max_frame_size(void) {
    int result = 1;
    uint8_t body[256];
    uint8_t* frame = NULL;
    uint8_t* exact_frame = NULL;
    size_t frame_len = 0;
    size_t exact_frame_len = 0;
    trevrpc_wire_response_values response = {0};

    for (size_t i = 0; i < sizeof(body); i++) {
        body[i] = (uint8_t)i;
    }

    int err = trevrpc_wire_encode_request(
        "svc", "method", TREVRPC_RPC_KIND_UNARY, body, sizeof(body), NULL, 0, 4096, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(frame_len > 4);
    size_t max_body_len = frame_len - 4;

    err = trevrpc_wire_encode_request("svc",
        "method",
        TREVRPC_RPC_KIND_UNARY,
        body,
        sizeof(body),
        NULL,
        0,
        max_body_len,
        &exact_frame,
        &exact_frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(exact_frame_len == frame_len);
    free(exact_frame);
    exact_frame = NULL;
    exact_frame_len = 0;

    err = trevrpc_wire_encode_request("svc",
        "method",
        TREVRPC_RPC_KIND_UNARY,
        body,
        sizeof(body),
        NULL,
        0,
        max_body_len - 1,
        &exact_frame,
        &exact_frame_len);
    CHECK_GOTO(err == TREVRPC_ERR_FRAME_TOO_LARGE);
    CHECK_GOTO(exact_frame == NULL);
    free(frame);
    frame = NULL;

    CHECK_GOTO(trevrpc_internal_response_set_body(&response, body, sizeof(body)) == 0);
    err = trevrpc_wire_encode_response(&response, 4096, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    max_body_len = frame_len - 4;
    err = trevrpc_wire_encode_response(&response, max_body_len, &exact_frame, &exact_frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(exact_frame_len == frame_len);
    free(exact_frame);
    exact_frame = NULL;
    exact_frame_len = 0;
    err = trevrpc_wire_encode_response(&response, max_body_len - 1, &exact_frame, &exact_frame_len);
    CHECK_GOTO(err == TREVRPC_ERR_FRAME_TOO_LARGE);
    CHECK_GOTO(exact_frame == NULL);
    free(frame);
    frame = NULL;

    err = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_MESSAGE,
        TREVRPC_STATUS_OK,
        NULL,
        0,
        body,
        sizeof(body),
        NULL,
        4096,
        &frame,
        &frame_len);
    CHECK_GOTO(err == 0);
    max_body_len = frame_len - 4;
    err = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_MESSAGE,
        TREVRPC_STATUS_OK,
        NULL,
        0,
        body,
        sizeof(body),
        NULL,
        max_body_len,
        &exact_frame,
        &exact_frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(exact_frame_len == frame_len);
    free(exact_frame);
    exact_frame = NULL;
    exact_frame_len = 0;
    err = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_MESSAGE,
        TREVRPC_STATUS_OK,
        NULL,
        0,
        body,
        sizeof(body),
        NULL,
        max_body_len - 1,
        &exact_frame,
        &exact_frame_len);
    CHECK_GOTO(err == TREVRPC_ERR_FRAME_TOO_LARGE);
    CHECK_GOTO(exact_frame == NULL);

    result = 0;

cleanup:
    trevrpc_internal_response_reset(&response);
    free(frame);
    free(exact_frame);
    return result;
}

static int test_decode_invalid_frame_corpus(void) {
    int result = 1;
    const uint8_t invalid_varint[] = {0x80};
    const uint8_t zero_tag[] = {0x00};
    const uint8_t truncated_string[] = {0x0a, 0x02, 0x61};
    const uint8_t truncated_nested[] = {0x22, 0x04, 0x0a, 0x03, 0x61};
    const struct {
        const char* name;
        const uint8_t* data;
        size_t len;
    } corpus[] = {
        {"invalid_varint", invalid_varint, sizeof(invalid_varint)},
        {"zero_tag", zero_tag, sizeof(zero_tag)},
        {"truncated_string", truncated_string, sizeof(truncated_string)},
        {"truncated_nested", truncated_nested, sizeof(truncated_nested)},
    };

    for (size_t i = 0; i < sizeof(corpus) / sizeof(corpus[0]); i++) {
        trevrpc_request request = {0};
        trevrpc_wire_response_values* response = NULL;
        trevrpc_wire_stream_frame_values* stream_frame = NULL;

        int err = trevrpc_wire_decode_request(corpus[i].data, corpus[i].len, &request);
        CHECK_GOTO(err == TREVRPC_ERR_INVALID_FRAME || err == TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION ||
                   err == TREVRPC_ERR_UNSUPPORTED_RPC_KIND);
        trevrpc_request_reset(&request);

        err = trevrpc_wire_decode_response(corpus[i].data, corpus[i].len, &response);
        CHECK_GOTO(err == TREVRPC_ERR_INVALID_FRAME);
        CHECK_GOTO(response == NULL);

        err = trevrpc_wire_decode_stream_frame(corpus[i].data, corpus[i].len, &stream_frame);
        CHECK_GOTO(err == TREVRPC_ERR_INVALID_FRAME);
        CHECK_GOTO(stream_frame == NULL);
    }

    result = 0;

cleanup:
    return result;
}

static int test_decode_rejects_truncated_valid_frames(void) {
    int result = 1;
    uint8_t* request_frame = NULL;
    uint8_t* response_frame = NULL;
    uint8_t* stream_frame = NULL;
    size_t request_frame_len = 0;
    size_t response_frame_len = 0;
    size_t stream_frame_len = 0;
    uint8_t body[] = {0x01, 0x02, 0x03};
    trevrpc_wire_response_values response = {0};

    int err = trevrpc_wire_encode_request(
        "svc", "method", TREVRPC_RPC_KIND_UNARY, body, sizeof(body), NULL, 0, 1024, &request_frame, &request_frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(trevrpc_internal_response_set_body(&response, body, sizeof(body)) == 0);
    err = trevrpc_wire_encode_response(&response, 1024, &response_frame, &response_frame_len);
    CHECK_GOTO(err == 0);
    err = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_MESSAGE,
        TREVRPC_STATUS_OK,
        NULL,
        0,
        body,
        sizeof(body),
        NULL,
        1024,
        &stream_frame,
        &stream_frame_len);
    CHECK_GOTO(err == 0);

    for (size_t len = 0; len + 1 < request_frame_len - 4; len++) {
        trevrpc_request decoded = {0};
        err = trevrpc_wire_decode_request(request_frame + 4, len, &decoded);
        CHECK_GOTO(err == TREVRPC_ERR_INVALID_FRAME || err == TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION ||
                   err == TREVRPC_ERR_UNSUPPORTED_RPC_KIND);
        trevrpc_request_reset(&decoded);
    }

    for (size_t len = 0; len + 1 < response_frame_len - 4; len++) {
        trevrpc_wire_response_values* decoded = NULL;
        err = trevrpc_wire_decode_response(response_frame + 4, len, &decoded);
        if (err == 0) {
            trevrpc_internal_response_free(decoded);
            continue;
        }
        CHECK_GOTO(err != 0);
        CHECK_GOTO(decoded == NULL);
    }

    for (size_t len = 0; len + 1 < stream_frame_len - 4; len++) {
        trevrpc_wire_stream_frame_values* decoded = NULL;
        err = trevrpc_wire_decode_stream_frame(stream_frame + 4, len, &decoded);
        if (err == 0) {
            trevrpc_internal_stream_frame_free(decoded);
            continue;
        }
        CHECK_GOTO(err != 0);
        CHECK_GOTO(decoded == NULL);
    }

    result = 0;

cleanup:
    trevrpc_internal_response_reset(&response);
    free(request_frame);
    free(response_frame);
    free(stream_frame);
    return result;
}

static int test_metadata_validation(void) {
    int result = 1;
    trevrpc_metadata metadata = {0};
    uint8_t value[] = {'o', 'k'};

    CHECK_GOTO(trevrpc_metadata_set(&metadata, "authorization", strlen("authorization"), value, sizeof(value)) == 0);
    CHECK_GOTO(trevrpc_metadata_validate(&metadata) == 0);
    CHECK_GOTO(metadata.entries_len == 1);
    CHECK_GOTO(metadata_value_equal(&metadata, "authorization", value, sizeof(value)));
    CHECK_GOTO(
        trevrpc_metadata_set(&metadata, "Authorization", strlen("Authorization"), value, sizeof(value)) == -EINVAL);
    trevrpc_metadata_reset(&metadata);

    trevrpc_metadata_entry duplicate_entries[] = {
        {.key = (char*)"dup", .key_len = 3, .value = value, .value_len = sizeof(value)},
        {.key = (char*)"dup", .key_len = 3, .value = value, .value_len = sizeof(value)},
    };
    trevrpc_metadata duplicate = {.entries = duplicate_entries, .entries_len = 2};
    CHECK_GOTO(trevrpc_metadata_validate(&duplicate) == -EINVAL);

    char entry_keys[TREVRPC_MAX_METADATA_ENTRIES + 1][4];
    trevrpc_metadata_entry too_many_entries[TREVRPC_MAX_METADATA_ENTRIES + 1];
    for (size_t i = 0; i <= TREVRPC_MAX_METADATA_ENTRIES; i++) {
        snprintf(entry_keys[i], sizeof(entry_keys[i]), "k%zu", i);
        too_many_entries[i] = (trevrpc_metadata_entry){
            .key = entry_keys[i],
            .key_len = strlen(entry_keys[i]),
        };
    }
    trevrpc_metadata too_many = {.entries = too_many_entries, .entries_len = TREVRPC_MAX_METADATA_ENTRIES + 1};
    CHECK_GOTO(trevrpc_metadata_validate(&too_many) == -EINVAL);

    char too_long_key[TREVRPC_MAX_METADATA_KEY_LEN + 1];
    memset(too_long_key, 'a', sizeof(too_long_key));
    trevrpc_metadata_entry key_too_long_entry = {
        .key = too_long_key,
        .key_len = sizeof(too_long_key),
    };
    trevrpc_metadata key_too_long = {.entries = &key_too_long_entry, .entries_len = 1};
    CHECK_GOTO(trevrpc_metadata_validate(&key_too_long) == -EINVAL);

    trevrpc_metadata_entry value_too_long_entry = {
        .key = (char*)"value-too-long",
        .key_len = strlen("value-too-long"),
        .value = value,
        .value_len = TREVRPC_MAX_METADATA_VALUE_LEN + 1,
    };
    trevrpc_metadata value_too_long = {.entries = &value_too_long_entry, .entries_len = 1};
    CHECK_GOTO(trevrpc_metadata_validate(&value_too_long) == -EINVAL);

    uint8_t large_value[TREVRPC_MAX_METADATA_VALUE_LEN - 1] = {0};
    char total_keys[] = "abcdefghi";
    trevrpc_metadata_entry total_entries[9];
    for (size_t i = 0; i < 9; i++) {
        total_entries[i] = (trevrpc_metadata_entry){
            .key = &total_keys[i],
            .key_len = 1,
            .value = large_value,
            .value_len = sizeof(large_value),
        };
    }
    trevrpc_metadata total_too_large = {.entries = total_entries, .entries_len = 9};
    CHECK_GOTO(trevrpc_metadata_validate(&total_too_large) == -EINVAL);

    result = 0;

cleanup:
    trevrpc_metadata_reset(&metadata);
    return result;
}

static int test_request_metadata_round_trip(void) {
    int result = 1;
    uint8_t body[] = {'h', 'i'};
    uint8_t value[] = {'o', 'k'};
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_metadata metadata = {0};
    trevrpc_request request = {0};
    uint8_t want_frame[] = {0x00,
        0x00,
        0x00,
        0x23,
        0x0a,
        0x03,
        's',
        'v',
        'c',
        0x12,
        0x01,
        'm',
        0x1a,
        0x02,
        'h',
        'i',
        0x22,
        0x13,
        0x0a,
        0x0d,
        'a',
        'u',
        't',
        'h',
        'o',
        'r',
        'i',
        'z',
        'a',
        't',
        'i',
        'o',
        'n',
        0x12,
        0x02,
        'o',
        'k',
        0x30,
        0x01};

    CHECK_GOTO(trevrpc_metadata_set(&metadata, "authorization", strlen("authorization"), value, sizeof(value)) == 0);
    int err = trevrpc_wire_encode_request(
        "svc", "m", TREVRPC_RPC_KIND_UNARY, body, sizeof(body), &metadata, 0, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(bytes_equal(frame, frame_len, want_frame, sizeof(want_frame)));

    err = trevrpc_wire_decode_request(frame + 4, frame_len - 4, &request);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(chars_equal(request.service, request.service_len, "svc"));
    CHECK_GOTO(chars_equal(request.method, request.method_len, "m"));
    CHECK_GOTO(bytes_equal(request.body, request.body_len, body, sizeof(body)));
    CHECK_GOTO(request.metadata.entries_len == 1);
    CHECK_GOTO(metadata_value_equal(&request.metadata, "authorization", value, sizeof(value)));

    result = 0;

cleanup:
    trevrpc_request_reset(&request);
    trevrpc_metadata_reset(&metadata);
    free(frame);
    return result;
}

static int test_request_parts_match_owned_frame_with_metadata(void) {
    int result = 1;
    trevrpc_request request = {
        .service = "svc",
        .service_len = 3,
        .method = "method",
        .method_len = 6,
        .body = (const uint8_t*)"payload",
        .body_len = 7,
        .kind = TREVRPC_RPC_KIND_SERVER_STREAMING,
        .version = TREVRPC_WIRE_VERSION,
        .timeout_nanos = 123456,
    };
    trevrpc_wire_frame_parts parts = {0};
    uint8_t metadata_value[] = {0x01, 0x02, 0x03};
    uint8_t* owned = NULL;
    size_t owned_len = 0;
    uint8_t* assembled = NULL;

    CHECK_GOTO(
        trevrpc_metadata_set(
            &request.metadata, "authorization", strlen("authorization"), metadata_value, sizeof(metadata_value)) == 0);
    CHECK_GOTO(trevrpc_wire_encode_request_view(request.service,
                   request.service_len,
                   request.method,
                   request.method_len,
                   request.kind,
                   request.version,
                   request.body,
                   request.body_len,
                   &request.metadata,
                   request.timeout_nanos,
                   1024,
                   &owned,
                   &owned_len) == 0);
    CHECK_GOTO(trevrpc_wire_encode_request_parts(&request, 1024, &parts) == 0);
    CHECK_GOTO(owned_len == 4 + parts.frame_body_len);

    assembled = malloc(owned_len);
    CHECK_GOTO(assembled != NULL);
    assembled[0] = (uint8_t)(parts.frame_body_len >> 24);
    assembled[1] = (uint8_t)(parts.frame_body_len >> 16);
    assembled[2] = (uint8_t)(parts.frame_body_len >> 8);
    assembled[3] = (uint8_t)parts.frame_body_len;
    memcpy(assembled + 4, parts.prefix, parts.prefix_len);
    memcpy(assembled + 4 + parts.prefix_len, parts.body, parts.body_len);
    memcpy(assembled + 4 + parts.prefix_len + parts.body_len, parts.suffix, parts.suffix_len);
    CHECK_GOTO(bytes_equal(assembled, owned_len, owned, owned_len));

    result = 0;

cleanup:
    free(assembled);
    free(owned);
    trevrpc_wire_frame_parts_reset(&parts);
    trevrpc_request_reset(&request);
    return result;
}

static int test_response_round_trip(void) {
    int result = 1;
    trevrpc_wire_response_values response = {0};
    trevrpc_wire_response_values* decoded = NULL;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    uint8_t body[] = {0x10, 0x20, 0x30};

    response.status = TREVRPC_STATUS_INVALID_ARGUMENT;
    CHECK_GOTO(trevrpc_internal_response_set_message(&response, "bad request", strlen("bad request")) == 0);
    CHECK_GOTO(trevrpc_internal_response_set_body(&response, body, sizeof(body)) == 0);

    int err = trevrpc_wire_encode_response(&response, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(frame != NULL);
    CHECK_GOTO(frame_body_len(frame) == frame_len - 4);

    err = trevrpc_wire_decode_response(frame + 4, frame_len - 4, &decoded);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(decoded != NULL);
    CHECK_GOTO(decoded->status == TREVRPC_STATUS_INVALID_ARGUMENT);
    CHECK_GOTO(chars_equal(decoded->message, decoded->message_len, "bad request"));
    CHECK_GOTO(bytes_equal(decoded->body.data, decoded->body.len, body, sizeof(body)));

    result = 0;

cleanup:
    trevrpc_internal_response_reset(&response);
    trevrpc_internal_response_free(decoded);
    free(frame);
    return result;
}

static int test_response_and_stream_frame_helpers(void) {
    int result = 1;
    trevrpc_wire_response_values response = {0};
    trevrpc_wire_stream_frame_values frame = {0};
    uint8_t first_body[] = {0x01, 0x02};
    uint8_t second_body[] = {0x03, 0x04, 0x05};

    CHECK_GOTO(trevrpc_internal_response_set_message(&response, "first", strlen("first")) == 0);
    CHECK_GOTO(chars_equal(response.message, response.message_len, "first"));
    CHECK_GOTO(trevrpc_internal_response_set_message(&response, "second", strlen("second")) == 0);
    CHECK_GOTO(chars_equal(response.message, response.message_len, "second"));
    CHECK_GOTO(trevrpc_internal_response_set_body(&response, first_body, sizeof(first_body)) == 0);
    CHECK_GOTO(bytes_equal(response.body.data, response.body.len, first_body, sizeof(first_body)));
    CHECK_GOTO(trevrpc_internal_response_set_body(&response, second_body, sizeof(second_body)) == 0);
    CHECK_GOTO(bytes_equal(response.body.data, response.body.len, second_body, sizeof(second_body)));
    CHECK_GOTO(trevrpc_internal_response_set_body(&response, NULL, 0) == 0);
    CHECK_GOTO(response.body.data == NULL);
    CHECK_GOTO(response.body.len == 0);
    trevrpc_internal_response_reset(&response);
    CHECK_GOTO(response.status == TREVRPC_STATUS_OK);
    CHECK_GOTO(response.message == NULL);
    CHECK_GOTO(response.body.data == NULL);
    trevrpc_internal_response_free(NULL);

    CHECK_GOTO(trevrpc_internal_stream_frame_set_message(&frame, "status", strlen("status")) == 0);
    CHECK_GOTO(chars_equal(frame.message, frame.message_len, "status"));
    CHECK_GOTO(trevrpc_internal_stream_frame_set_body(&frame, first_body, sizeof(first_body)) == 0);
    CHECK_GOTO(bytes_equal(frame.body.data, frame.body.len, first_body, sizeof(first_body)));
    CHECK_GOTO(trevrpc_internal_stream_frame_set_body(&frame, NULL, 0) == 0);
    CHECK_GOTO(frame.body.data == NULL);
    CHECK_GOTO(frame.body.len == 0);
    frame.kind = TREVRPC_STREAM_FRAME_KIND_STATUS;
    frame.status = TREVRPC_STATUS_UNAVAILABLE;
    trevrpc_internal_stream_frame_reset(&frame);
    CHECK_GOTO(frame.kind == TREVRPC_STREAM_FRAME_KIND_MESSAGE);
    CHECK_GOTO(frame.status == TREVRPC_STATUS_OK);
    CHECK_GOTO(frame.message == NULL);
    CHECK_GOTO(frame.body.data == NULL);
    trevrpc_internal_stream_frame_free(NULL);

    result = 0;

cleanup:
    trevrpc_internal_response_reset(&response);
    trevrpc_internal_stream_frame_reset(&frame);
    return result;
}

static int test_status_helpers(void) {
    int result = 1;
    trevrpc_wire_response_values response = {0};
    trevrpc_wire_stream_frame_values frame = {0};
    uint8_t body[] = {0x01};

    CHECK_GOTO(trevrpc_status_code_from_uint32(TREVRPC_STATUS_OK) == TREVRPC_STATUS_OK);
    CHECK_GOTO(trevrpc_status_code_from_uint32(TREVRPC_STATUS_UNAUTHENTICATED) == TREVRPC_STATUS_UNAUTHENTICATED);
    CHECK_GOTO(trevrpc_status_code_from_uint32(999) == TREVRPC_STATUS_UNKNOWN);
    CHECK_GOTO(strcmp(trevrpc_status_code_string(TREVRPC_STATUS_DEADLINE_EXCEEDED), "DeadlineExceeded") == 0);
    CHECK_GOTO(strcmp(trevrpc_status_code_string(999), "Unknown") == 0);

    trevrpc_status ok = trevrpc_status_ok();
    CHECK_GOTO(ok.code == TREVRPC_STATUS_OK);
    CHECK_GOTO(ok.message == NULL);
    CHECK_GOTO(ok.message_len == 0);

    trevrpc_status status = trevrpc_status_permission_denied("no", strlen("no"));
    CHECK_GOTO(status.code == TREVRPC_STATUS_PERMISSION_DENIED);
    CHECK_GOTO(status.message_len == strlen("no"));

    CHECK_GOTO(
        trevrpc_internal_response_set_status(&response, trevrpc_status_unavailable("down", strlen("down"))) == 0);
    CHECK_GOTO(response.status == TREVRPC_STATUS_UNAVAILABLE);
    CHECK_GOTO(chars_equal(response.message, response.message_len, "down"));
    CHECK_GOTO(trevrpc_internal_response_set_status(&response, trevrpc_status_new(999, "odd", strlen("odd"))) == 0);
    CHECK_GOTO(response.status == TREVRPC_STATUS_UNKNOWN);
    CHECK_GOTO(chars_equal(response.message, response.message_len, "odd"));

    CHECK_GOTO(trevrpc_internal_stream_frame_set_body(&frame, body, sizeof(body)) == 0);
    CHECK_GOTO(trevrpc_internal_stream_frame_set_status(&frame, trevrpc_status_internal("boom", strlen("boom"))) == 0);
    CHECK_GOTO(frame.kind == TREVRPC_STREAM_FRAME_KIND_STATUS);
    CHECK_GOTO(frame.status == TREVRPC_STATUS_INTERNAL);
    CHECK_GOTO(chars_equal(frame.message, frame.message_len, "boom"));
    CHECK_GOTO(frame.body.data == NULL);
    CHECK_GOTO(frame.body.len == 0);

    result = 0;

cleanup:
    trevrpc_internal_response_reset(&response);
    trevrpc_internal_stream_frame_reset(&frame);
    return result;
}

static int test_response_metadata_round_trip(void) {
    int result = 1;
    trevrpc_wire_response_values response = {0};
    trevrpc_wire_response_values* decoded = NULL;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    uint8_t body[] = {'o', 'k'};
    uint8_t trace_id[] = {0x01, 0x02, 0x03};

    CHECK_GOTO(trevrpc_internal_response_set_body(&response, body, sizeof(body)) == 0);
    CHECK_GOTO(
        trevrpc_metadata_set(&response.metadata, "trace-id", strlen("trace-id"), trace_id, sizeof(trace_id)) == 0);

    int err = trevrpc_wire_encode_response(&response, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    err = trevrpc_wire_decode_response(frame + 4, frame_len - 4, &decoded);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(decoded->status == TREVRPC_STATUS_OK);
    CHECK_GOTO(bytes_equal(decoded->body.data, decoded->body.len, body, sizeof(body)));
    CHECK_GOTO(decoded->metadata.entries_len == 1);
    CHECK_GOTO(metadata_value_equal(&decoded->metadata, "trace-id", trace_id, sizeof(trace_id)));

    result = 0;

cleanup:
    trevrpc_internal_response_reset(&response);
    trevrpc_internal_response_free(decoded);
    free(frame);
    return result;
}

static int test_stream_frame_round_trip(void) {
    int result = 1;
    trevrpc_wire_stream_frame_values* decoded = NULL;
    uint8_t* frame = NULL;
    size_t frame_len = 0;

    int err = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_STATUS,
        TREVRPC_STATUS_UNAVAILABLE,
        "retry later",
        strlen("retry later"),
        NULL,
        0,
        NULL,
        1024,
        &frame,
        &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(frame != NULL);
    CHECK_GOTO(frame_body_len(frame) == frame_len - 4);

    err = trevrpc_wire_decode_stream_frame(frame + 4, frame_len - 4, &decoded);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(decoded != NULL);
    CHECK_GOTO(decoded->kind == TREVRPC_STREAM_FRAME_KIND_STATUS);
    CHECK_GOTO(decoded->status == TREVRPC_STATUS_UNAVAILABLE);
    CHECK_GOTO(chars_equal(decoded->message, decoded->message_len, "retry later"));
    CHECK_GOTO(decoded->body.len == 0);

    result = 0;

cleanup:
    trevrpc_internal_stream_frame_free(decoded);
    free(frame);
    return result;
}

static int test_stream_message_frame_take_avoids_body_copy(void) {
    int result = 1;
    trevrpc_wire_stream_frame_values* decoded = NULL;
    uint8_t* frame = NULL;
    uint8_t* owner = NULL;
    size_t frame_len = 0;
    uint8_t payload[] = {'h', 'e', 'l', 'l', 'o'};

    int err = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_MESSAGE,
        TREVRPC_STATUS_OK,
        NULL,
        0,
        payload,
        sizeof(payload),
        NULL,
        1024,
        &frame,
        &frame_len);
    CHECK_GOTO(err == 0);

    size_t body_len = frame_len - 4;
    owner = malloc(body_len);
    CHECK_GOTO(owner != NULL);
    memcpy(owner, frame + 4, body_len);

    bool took_body = false;
    err = trevrpc_wire_decode_stream_frame_take(owner, body_len, &decoded, &took_body);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(took_body);
    CHECK_GOTO(decoded != NULL);
    CHECK_GOTO(decoded->kind == TREVRPC_STREAM_FRAME_KIND_MESSAGE);
    CHECK_GOTO(bytes_equal(decoded->body.data, decoded->body.len, payload, sizeof(payload)));
    CHECK_GOTO(decoded->body.owner == owner);
    CHECK_GOTO(decoded->body.data > owner && decoded->body.data < owner + body_len);
    owner = NULL;

    result = 0;

cleanup:
    trevrpc_internal_stream_frame_free(decoded);
    free(owner);
    free(frame);
    return result;
}

static int test_stream_frame_metadata_round_trip(void) {
    int result = 1;
    trevrpc_wire_stream_frame_values* decoded = NULL;
    trevrpc_metadata metadata = {0};
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    uint8_t trailer[] = {'d', 'o', 'n', 'e'};

    CHECK_GOTO(trevrpc_metadata_set(&metadata, "trailer", strlen("trailer"), trailer, sizeof(trailer)) == 0);
    int err = trevrpc_wire_encode_stream_frame(
        TREVRPC_STREAM_FRAME_KIND_STATUS, TREVRPC_STATUS_OK, NULL, 0, NULL, 0, &metadata, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    err = trevrpc_wire_decode_stream_frame(frame + 4, frame_len - 4, &decoded);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(decoded->kind == TREVRPC_STREAM_FRAME_KIND_STATUS);
    CHECK_GOTO(decoded->metadata.entries_len == 1);
    CHECK_GOTO(metadata_value_equal(&decoded->metadata, "trailer", trailer, sizeof(trailer)));

    result = 0;

cleanup:
    trevrpc_internal_stream_frame_free(decoded);
    trevrpc_metadata_reset(&metadata);
    free(frame);
    return result;
}

static int test_metadata_decode_rejects_invalid_keys(void) {
    int result = 1;
    trevrpc_request request = {0};
    uint8_t invalid_metadata_request[] = {0x0a,
        0x03,
        's',
        'v',
        'c',
        0x12,
        0x01,
        'm',
        0x22,
        0x13,
        0x0a,
        0x0d,
        'A',
        'u',
        't',
        'h',
        'o',
        'r',
        'i',
        'z',
        'a',
        't',
        'i',
        'o',
        'n',
        0x12,
        0x02,
        'o',
        'k',
        0x30,
        0x01};

    int err = trevrpc_wire_decode_request(invalid_metadata_request, sizeof(invalid_metadata_request), &request);
    CHECK_GOTO(err == TREVRPC_ERR_INVALID_FRAME);

    result = 0;

cleanup:
    trevrpc_request_reset(&request);
    return result;
}

static int test_metadata_decode_uses_last_duplicate_key(void) {
    int result = 1;
    trevrpc_request request = {0};
    uint8_t want_value[] = {'n', 'e', 'w'};
    uint8_t duplicate_metadata_request[] = {0x0a,
        0x03,
        's',
        'v',
        'c',
        0x12,
        0x01,
        'm',
        0x1a,
        0x02,
        'h',
        'i',
        0x22,
        0x14,
        0x0a,
        0x0d,
        'a',
        'u',
        't',
        'h',
        'o',
        'r',
        'i',
        'z',
        'a',
        't',
        'i',
        'o',
        'n',
        0x12,
        0x03,
        'o',
        'l',
        'd',
        0x22,
        0x14,
        0x0a,
        0x0d,
        'a',
        'u',
        't',
        'h',
        'o',
        'r',
        'i',
        'z',
        'a',
        't',
        'i',
        'o',
        'n',
        0x12,
        0x03,
        'n',
        'e',
        'w',
        0x30,
        0x01};

    int err = trevrpc_wire_decode_request(duplicate_metadata_request, sizeof(duplicate_metadata_request), &request);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(request.metadata.entries_len == 1);
    CHECK_GOTO(metadata_value_equal(&request.metadata, "authorization", want_value, sizeof(want_value)));

    result = 0;

cleanup:
    trevrpc_request_reset(&request);
    return result;
}

static int test_metadata_decode_skips_unknown_entry_fields(void) {
    int result = 1;
    trevrpc_request request = {0};
    uint8_t value[] = {'o', 'k'};
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    uint8_t metadata_with_unknown[] = {0x0a,
        0x03,
        's',
        'v',
        'c',
        0x12,
        0x01,
        'm',
        0x22,
        0x15,
        0x0a,
        0x0d,
        'a',
        'u',
        't',
        'h',
        'o',
        'r',
        'i',
        'z',
        'a',
        't',
        'i',
        'o',
        'n',
        0x12,
        0x02,
        'o',
        'k',
        0x48,
        0x07,
        0x30,
        0x01};
    uint8_t expected_reencoded[] = {0x00,
        0x00,
        0x00,
        0x1f,
        0x0a,
        0x03,
        's',
        'v',
        'c',
        0x12,
        0x01,
        'm',
        0x22,
        0x13,
        0x0a,
        0x0d,
        'a',
        'u',
        't',
        'h',
        'o',
        'r',
        'i',
        'z',
        'a',
        't',
        'i',
        'o',
        'n',
        0x12,
        0x02,
        'o',
        'k',
        0x30,
        0x01};

    int err = trevrpc_wire_decode_request(metadata_with_unknown, sizeof(metadata_with_unknown), &request);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(request.metadata.entries_len == 1);
    CHECK_GOTO(metadata_value_equal(&request.metadata, "authorization", value, sizeof(value)));

    err = trevrpc_wire_encode_request(
        "svc", "m", TREVRPC_RPC_KIND_UNARY, NULL, 0, &request.metadata, 0, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(bytes_equal(frame, frame_len, expected_reencoded, sizeof(expected_reencoded)));

    result = 0;

cleanup:
    trevrpc_request_reset(&request);
    free(frame);
    return result;
}

static int test_normative_wire_diagnostics_and_raw_status(void) {
    int result = 1;
    const uint8_t request_wrong_wire_type[] = {0x08, 0x01};
    const uint8_t response_truncated_varint[] = {0x80};
    const uint8_t response_wrong_wire_type[] = {0x0a, 0x00};
    const uint8_t response_invalid_utf8[] = {0x12, 0x01, 0xff};
    const uint8_t response_invalid_metadata[] = {
        0x22,
        0x13,
        0x0a,
        0x0d,
        'A',
        'u',
        't',
        'h',
        'o',
        'r',
        'i',
        'z',
        'a',
        't',
        'i',
        'o',
        'n',
        0x12,
        0x02,
        'o',
        'k',
    };
    const uint8_t stream_invalid_metadata[] = {
        0x2a,
        0x13,
        0x0a,
        0x0d,
        'A',
        'u',
        't',
        'h',
        'o',
        'r',
        'i',
        'z',
        'a',
        't',
        'i',
        'o',
        'n',
        0x12,
        0x02,
        'o',
        'k',
    };
    const uint8_t stream_unsupported_kind[] = {0x08, 0x02};
    const uint8_t response_uint32_overflow[] = {0x08, 0xff, 0xff, 0xff, 0xff, 0x1f};
    const uint8_t unknown_status_body[] = {0x08, 0x01, 0x10, 0xe7, 0x07, 0x1a, 0x03, 'o', 'd', 'd'};
    trevrpc_request request = {0};
    trevrpc_wire_response_values* response = NULL;
    trevrpc_wire_stream_frame_values* stream_frame = NULL;
    trevrpc_wire_request_diagnostic request_diagnostic = {0};
    trevrpc_wire_diagnostic diagnostic = {0};
    uint8_t* encoded = NULL;
    size_t encoded_len = 0;

    CHECK_GOTO(trevrpc_wire_decode_request_diagnostic(
                   request_wrong_wire_type, sizeof(request_wrong_wire_type), &request, &request_diagnostic) ==
               TREVRPC_ERR_INVALID_FRAME);
    CHECK_GOTO(request_diagnostic.reason == TREVRPC_WIRE_DIAGNOSTIC_WRONG_WIRE_TYPE);

    CHECK_GOTO(trevrpc_wire_decode_response_diagnostic(
                   response_truncated_varint, sizeof(response_truncated_varint), &response, &diagnostic) ==
               TREVRPC_ERR_INVALID_FRAME);
    CHECK_GOTO(diagnostic.reason == TREVRPC_WIRE_DIAGNOSTIC_MALFORMED_PROTOBUF);
    CHECK_GOTO(trevrpc_wire_decode_response_diagnostic(
                   response_wrong_wire_type, sizeof(response_wrong_wire_type), &response, &diagnostic) ==
               TREVRPC_ERR_INVALID_FRAME);
    CHECK_GOTO(diagnostic.reason == TREVRPC_WIRE_DIAGNOSTIC_WRONG_WIRE_TYPE);
    CHECK_GOTO(
        trevrpc_wire_decode_response_diagnostic(
            response_invalid_utf8, sizeof(response_invalid_utf8), &response, &diagnostic) == TREVRPC_ERR_INVALID_FRAME);
    CHECK_GOTO(diagnostic.reason == TREVRPC_WIRE_DIAGNOSTIC_INVALID_UTF8);
    CHECK_GOTO(trevrpc_wire_decode_response_diagnostic(
                   response_invalid_metadata, sizeof(response_invalid_metadata), &response, &diagnostic) ==
               TREVRPC_ERR_INVALID_FRAME);
    CHECK_GOTO(diagnostic.reason == TREVRPC_WIRE_DIAGNOSTIC_INVALID_METADATA);
    CHECK_GOTO(trevrpc_wire_decode_response_diagnostic(
                   response_uint32_overflow, sizeof(response_uint32_overflow), &response, &diagnostic) ==
               TREVRPC_ERR_INVALID_FRAME);
    CHECK_GOTO(diagnostic.reason == TREVRPC_WIRE_DIAGNOSTIC_UINT32_OVERFLOW);

    CHECK_GOTO(trevrpc_wire_decode_stream_frame_diagnostic(
                   stream_invalid_metadata, sizeof(stream_invalid_metadata), &stream_frame, &diagnostic) ==
               TREVRPC_ERR_INVALID_FRAME);
    CHECK_GOTO(diagnostic.reason == TREVRPC_WIRE_DIAGNOSTIC_INVALID_METADATA);
    CHECK_GOTO(trevrpc_wire_decode_stream_frame_diagnostic(
                   stream_unsupported_kind, sizeof(stream_unsupported_kind), &stream_frame, &diagnostic) ==
               TREVRPC_ERR_INVALID_FRAME);
    CHECK_GOTO(diagnostic.reason == TREVRPC_WIRE_DIAGNOSTIC_UNSUPPORTED_FRAME_KIND);

    CHECK_GOTO(trevrpc_wire_decode_stream_frame_diagnostic(
                   unknown_status_body, sizeof(unknown_status_body), &stream_frame, &diagnostic) == 0);
    CHECK_GOTO(stream_frame != NULL);
    CHECK_GOTO(stream_frame->kind == TREVRPC_STREAM_FRAME_KIND_STATUS);
    CHECK_GOTO(stream_frame->status == 999);
    CHECK_GOTO(trevrpc_status_code_from_uint32(stream_frame->status) == TREVRPC_STATUS_UNKNOWN);
    CHECK_GOTO(chars_equal(stream_frame->message, stream_frame->message_len, "odd"));
    CHECK_GOTO(trevrpc_wire_encode_stream_frame(stream_frame->kind,
                   stream_frame->status,
                   stream_frame->message,
                   stream_frame->message_len,
                   stream_frame->body.data,
                   stream_frame->body.len,
                   &stream_frame->metadata,
                   1024,
                   &encoded,
                   &encoded_len) == 0);
    CHECK_GOTO(encoded_len == sizeof(unknown_status_body) + 4);
    CHECK_GOTO(memcmp(encoded + 4, unknown_status_body, sizeof(unknown_status_body)) == 0);
    free(encoded);
    encoded = NULL;
    trevrpc_internal_stream_frame_free(stream_frame);
    stream_frame = NULL;

    CHECK_GOTO(trevrpc_wire_encode_request(
                   "", "", TREVRPC_RPC_KIND_UNARY, NULL, 0, NULL, 0, 1024, &encoded, &encoded_len) == 0);
    CHECK_GOTO(trevrpc_wire_decode_request(encoded + 4, encoded_len - 4, &request) == 0);
    CHECK_GOTO(request.service != NULL && request.service_len == 0);
    CHECK_GOTO(request.method != NULL && request.method_len == 0);

    result = 0;

cleanup:
    free(encoded);
    trevrpc_request_reset(&request);
    trevrpc_internal_response_free(response);
    trevrpc_internal_stream_frame_free(stream_frame);
    return result;
}

static int test_wire_golden_vectors(void) {
    int result = 1;
    uint8_t body[] = {'h', 'i'};
    uint8_t metadata_value[] = {'o', 'k'};
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_metadata metadata = {0};
    trevrpc_wire_response_values response = {0};

    int err = trevrpc_wire_encode_request(
        "svc", "m", TREVRPC_RPC_KIND_UNARY, body, sizeof(body), NULL, 0, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(assert_golden_message("rpc_request.unary", frame, frame_len) == 0);
    free(frame);
    frame = NULL;

    err = trevrpc_wire_encode_request(
        "svc", "m", TREVRPC_RPC_KIND_UNARY, body, sizeof(body), NULL, 123456, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(assert_golden_message("rpc_request.timeout", frame, frame_len) == 0);
    free(frame);
    frame = NULL;

    CHECK_GOTO(trevrpc_metadata_set(
                   &metadata, "authorization", strlen("authorization"), metadata_value, sizeof(metadata_value)) == 0);
    err = trevrpc_wire_encode_request(
        "svc", "m", TREVRPC_RPC_KIND_UNARY, body, sizeof(body), &metadata, 0, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(assert_golden_message("rpc_request.metadata", frame, frame_len) == 0);
    free(frame);
    frame = NULL;

    err = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_MESSAGE,
        TREVRPC_STATUS_OK,
        NULL,
        0,
        body,
        sizeof(body),
        NULL,
        1024,
        &frame,
        &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(assert_golden_message("rpc_stream_frame.message", frame, frame_len) == 0);
    free(frame);
    frame = NULL;

    err = trevrpc_wire_encode_stream_frame(TREVRPC_STREAM_FRAME_KIND_STATUS,
        TREVRPC_STATUS_UNAVAILABLE,
        "down",
        strlen("down"),
        NULL,
        0,
        NULL,
        1024,
        &frame,
        &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(assert_golden_message("rpc_stream_frame.status", frame, frame_len) == 0);
    free(frame);
    frame = NULL;

    CHECK_GOTO(trevrpc_internal_response_set_body(&response, body, sizeof(body)) == 0);
    err = trevrpc_wire_encode_response(&response, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(assert_golden_message("rpc_response.ok_body", frame, frame_len) == 0);
    free(frame);
    frame = NULL;
    trevrpc_internal_response_reset(&response);

    response.status = TREVRPC_STATUS_UNAVAILABLE;
    CHECK_GOTO(trevrpc_internal_response_set_message(&response, "down", strlen("down")) == 0);
    err = trevrpc_wire_encode_response(&response, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(assert_golden_message("rpc_response.unavailable", frame, frame_len) == 0);

    result = 0;

cleanup:
    trevrpc_internal_response_reset(&response);
    trevrpc_metadata_reset(&metadata);
    free(frame);
    return result;
}

static int test_typed_bytes_field_canonicalization(void) {
    int result = 1;
    uint8_t* canonical = NULL;
    size_t canonical_len = 0;
    const uint8_t known_with_unknown[] = {0x28, 0x01, 0x1a, 0x02, 'o', 'k'};
    const uint8_t expected[] = {0x1a, 0x02, 'o', 'k'};
    const uint8_t wrong_known_wire[] = {0x18, 0x01};

    CHECK_GOTO(trevrpc_wire_canonicalize_bytes_field(
                   known_with_unknown, sizeof(known_with_unknown), 3, &canonical, &canonical_len) == 0);
    CHECK_GOTO(bytes_equal(canonical, canonical_len, expected, sizeof(expected)));
    free(canonical);
    canonical = NULL;
    canonical_len = 0;

    CHECK_GOTO(trevrpc_wire_canonicalize_bytes_field(
                   wrong_known_wire, sizeof(wrong_known_wire), 3, &canonical, &canonical_len) == -EINVAL);

    result = 0;

cleanup:
    free(canonical);
    return result;
}

int main(void) {
    if (test_request_round_trip() != 0) {
        return 1;
    }
    if (test_request_rejects_bad_frames() != 0) {
        return 1;
    }
    if (test_request_diagnostics_preserve_reliable_kind() != 0) {
        return 1;
    }
    if (test_encode_rejects_oversized_frames() != 0) {
        return 1;
    }
    if (test_encode_accepts_exact_max_frame_size() != 0) {
        return 1;
    }
    if (test_decode_invalid_frame_corpus() != 0) {
        return 1;
    }
    if (test_decode_rejects_truncated_valid_frames() != 0) {
        return 1;
    }
    if (test_metadata_validation() != 0) {
        return 1;
    }
    if (test_request_metadata_round_trip() != 0) {
        return 1;
    }
    if (test_request_parts_match_owned_frame_with_metadata() != 0) {
        return 1;
    }
    if (test_response_round_trip() != 0) {
        return 1;
    }
    if (test_response_and_stream_frame_helpers() != 0) {
        return 1;
    }
    if (test_status_helpers() != 0) {
        return 1;
    }
    if (test_response_metadata_round_trip() != 0) {
        return 1;
    }
    if (test_stream_frame_round_trip() != 0) {
        return 1;
    }
    if (test_stream_message_frame_take_avoids_body_copy() != 0) {
        return 1;
    }
    if (test_stream_frame_metadata_round_trip() != 0) {
        return 1;
    }
    if (test_metadata_decode_rejects_invalid_keys() != 0) {
        return 1;
    }
    if (test_metadata_decode_uses_last_duplicate_key() != 0) {
        return 1;
    }
    if (test_metadata_decode_skips_unknown_entry_fields() != 0) {
        return 1;
    }
    if (test_normative_wire_diagnostics_and_raw_status() != 0) {
        return 1;
    }
    if (test_typed_bytes_field_canonicalization() != 0) {
        return 1;
    }
    if (test_wire_golden_vectors() != 0) {
        return 1;
    }
    return 0;
}
