#include "trevrpc.h"
#include "trevrpc_wire.h"

#include <errno.h>
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

static int test_response_round_trip(void) {
    int result = 1;
    trevrpc_response response = {0};
    trevrpc_response* decoded = NULL;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    uint8_t body[] = {0x10, 0x20, 0x30};

    response.status = TREVRPC_STATUS_INVALID_ARGUMENT;
    CHECK_GOTO(trevrpc_response_set_message(&response, "bad request", strlen("bad request")) == 0);
    CHECK_GOTO(trevrpc_response_set_body(&response, body, sizeof(body)) == 0);

    int err = trevrpc_wire_encode_response(&response, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(frame != NULL);
    CHECK_GOTO(frame_body_len(frame) == frame_len - 4);

    err = trevrpc_wire_decode_response(frame + 4, frame_len - 4, &decoded);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(decoded != NULL);
    CHECK_GOTO(decoded->status == TREVRPC_STATUS_INVALID_ARGUMENT);
    CHECK_GOTO(chars_equal(decoded->message, decoded->message_len, "bad request"));
    CHECK_GOTO(bytes_equal(decoded->body, decoded->body_len, body, sizeof(body)));

    result = 0;

cleanup:
    trevrpc_response_reset(&response);
    trevrpc_response_free(decoded);
    free(frame);
    return result;
}

static int test_response_and_stream_frame_helpers(void) {
    int result = 1;
    trevrpc_response response = {0};
    trevrpc_stream_frame frame = {0};
    uint8_t first_body[] = {0x01, 0x02};
    uint8_t second_body[] = {0x03, 0x04, 0x05};

    CHECK_GOTO(trevrpc_response_set_message(&response, "first", strlen("first")) == 0);
    CHECK_GOTO(chars_equal(response.message, response.message_len, "first"));
    CHECK_GOTO(trevrpc_response_set_message(&response, "second", strlen("second")) == 0);
    CHECK_GOTO(chars_equal(response.message, response.message_len, "second"));
    CHECK_GOTO(trevrpc_response_set_body(&response, first_body, sizeof(first_body)) == 0);
    CHECK_GOTO(bytes_equal(response.body, response.body_len, first_body, sizeof(first_body)));
    CHECK_GOTO(trevrpc_response_set_body(&response, second_body, sizeof(second_body)) == 0);
    CHECK_GOTO(bytes_equal(response.body, response.body_len, second_body, sizeof(second_body)));
    CHECK_GOTO(trevrpc_response_set_body(&response, NULL, 0) == 0);
    CHECK_GOTO(response.body == NULL);
    CHECK_GOTO(response.body_len == 0);
    trevrpc_response_reset(&response);
    CHECK_GOTO(response.status == TREVRPC_STATUS_OK);
    CHECK_GOTO(response.message == NULL);
    CHECK_GOTO(response.body == NULL);
    trevrpc_response_free(NULL);

    CHECK_GOTO(trevrpc_stream_frame_set_message(&frame, "status", strlen("status")) == 0);
    CHECK_GOTO(chars_equal(frame.message, frame.message_len, "status"));
    CHECK_GOTO(trevrpc_stream_frame_set_body(&frame, first_body, sizeof(first_body)) == 0);
    CHECK_GOTO(bytes_equal(frame.body, frame.body_len, first_body, sizeof(first_body)));
    CHECK_GOTO(trevrpc_stream_frame_set_body(&frame, NULL, 0) == 0);
    CHECK_GOTO(frame.body == NULL);
    CHECK_GOTO(frame.body_len == 0);
    frame.kind = TREVRPC_STREAM_FRAME_KIND_STATUS;
    frame.status = TREVRPC_STATUS_UNAVAILABLE;
    trevrpc_stream_frame_reset(&frame);
    CHECK_GOTO(frame.kind == TREVRPC_STREAM_FRAME_KIND_MESSAGE);
    CHECK_GOTO(frame.status == TREVRPC_STATUS_OK);
    CHECK_GOTO(frame.message == NULL);
    CHECK_GOTO(frame.body == NULL);
    trevrpc_stream_frame_free(NULL);

    result = 0;

cleanup:
    trevrpc_response_reset(&response);
    trevrpc_stream_frame_reset(&frame);
    return result;
}

static int test_status_helpers(void) {
    int result = 1;
    trevrpc_response response = {0};
    trevrpc_stream_frame frame = {0};
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

    CHECK_GOTO(trevrpc_response_set_status(&response, trevrpc_status_unavailable("down", strlen("down"))) == 0);
    CHECK_GOTO(response.status == TREVRPC_STATUS_UNAVAILABLE);
    CHECK_GOTO(chars_equal(response.message, response.message_len, "down"));
    CHECK_GOTO(trevrpc_response_set_status(&response, trevrpc_status_new(999, "odd", strlen("odd"))) == 0);
    CHECK_GOTO(response.status == TREVRPC_STATUS_UNKNOWN);
    CHECK_GOTO(chars_equal(response.message, response.message_len, "odd"));

    CHECK_GOTO(trevrpc_stream_frame_set_body(&frame, body, sizeof(body)) == 0);
    CHECK_GOTO(trevrpc_stream_frame_set_status(&frame, trevrpc_status_internal("boom", strlen("boom"))) == 0);
    CHECK_GOTO(frame.kind == TREVRPC_STREAM_FRAME_KIND_STATUS);
    CHECK_GOTO(frame.status == TREVRPC_STATUS_INTERNAL);
    CHECK_GOTO(chars_equal(frame.message, frame.message_len, "boom"));
    CHECK_GOTO(frame.body == NULL);
    CHECK_GOTO(frame.body_len == 0);

    result = 0;

cleanup:
    trevrpc_response_reset(&response);
    trevrpc_stream_frame_reset(&frame);
    return result;
}

static int test_response_metadata_round_trip(void) {
    int result = 1;
    trevrpc_response response = {0};
    trevrpc_response* decoded = NULL;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    uint8_t body[] = {'o', 'k'};
    uint8_t trace_id[] = {0x01, 0x02, 0x03};

    CHECK_GOTO(trevrpc_response_set_body(&response, body, sizeof(body)) == 0);
    CHECK_GOTO(
        trevrpc_metadata_set(&response.metadata, "trace-id", strlen("trace-id"), trace_id, sizeof(trace_id)) == 0);

    int err = trevrpc_wire_encode_response(&response, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    err = trevrpc_wire_decode_response(frame + 4, frame_len - 4, &decoded);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(decoded->status == TREVRPC_STATUS_OK);
    CHECK_GOTO(bytes_equal(decoded->body, decoded->body_len, body, sizeof(body)));
    CHECK_GOTO(decoded->metadata.entries_len == 1);
    CHECK_GOTO(metadata_value_equal(&decoded->metadata, "trace-id", trace_id, sizeof(trace_id)));

    result = 0;

cleanup:
    trevrpc_response_reset(&response);
    trevrpc_response_free(decoded);
    free(frame);
    return result;
}

static int test_stream_frame_round_trip(void) {
    int result = 1;
    trevrpc_stream_frame* decoded = NULL;
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
    CHECK_GOTO(decoded->body_len == 0);

    result = 0;

cleanup:
    trevrpc_stream_frame_free(decoded);
    free(frame);
    return result;
}

static int test_stream_frame_metadata_round_trip(void) {
    int result = 1;
    trevrpc_stream_frame* decoded = NULL;
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
    trevrpc_stream_frame_free(decoded);
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

static int test_wire_golden_vectors(void) {
    int result = 1;
    uint8_t body[] = {'h', 'i'};
    uint8_t metadata_value[] = {'o', 'k'};
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_metadata metadata = {0};
    trevrpc_response response = {0};

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

    CHECK_GOTO(trevrpc_response_set_body(&response, body, sizeof(body)) == 0);
    err = trevrpc_wire_encode_response(&response, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(assert_golden_message("rpc_response.ok_body", frame, frame_len) == 0);
    free(frame);
    frame = NULL;
    trevrpc_response_reset(&response);

    response.status = TREVRPC_STATUS_UNAVAILABLE;
    CHECK_GOTO(trevrpc_response_set_message(&response, "down", strlen("down")) == 0);
    err = trevrpc_wire_encode_response(&response, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(assert_golden_message("rpc_response.unavailable", frame, frame_len) == 0);

    result = 0;

cleanup:
    trevrpc_response_reset(&response);
    trevrpc_metadata_reset(&metadata);
    free(frame);
    return result;
}

int main(void) {
    if (test_request_round_trip() != 0) {
        return 1;
    }
    if (test_request_rejects_bad_frames() != 0) {
        return 1;
    }
    if (test_encode_rejects_oversized_frames() != 0) {
        return 1;
    }
    if (test_metadata_validation() != 0) {
        return 1;
    }
    if (test_request_metadata_round_trip() != 0) {
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
    if (test_wire_golden_vectors() != 0) {
        return 1;
    }
    return 0;
}
