#include "trevrpc.h"
#include "trevrpc_wire.h"

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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

static int test_wire_golden_vectors(void) {
    int result = 1;
    uint8_t body[] = {'h', 'i'};
    uint8_t metadata_value[] = {'o', 'k'};
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_metadata metadata = {0};
    trevrpc_response response = {0};

    uint8_t request_unary[] = {
        0x00, 0x00, 0x00, 0x0e, 0x0a, 0x03, 's', 'v', 'c', 0x12, 0x01, 'm', 0x1a, 0x02, 'h', 'i', 0x30, 0x01};
    uint8_t request_timeout[] = {0x00,
        0x00,
        0x00,
        0x12,
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
        0x30,
        0x01,
        0x38,
        0xc0,
        0xc4,
        0x07};
    uint8_t request_metadata[] = {0x00,
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
    uint8_t stream_message[] = {0x00, 0x00, 0x00, 0x04, 0x22, 0x02, 'h', 'i'};
    uint8_t stream_status[] = {0x00, 0x00, 0x00, 0x0a, 0x08, 0x01, 0x10, 0x0e, 0x1a, 0x04, 'd', 'o', 'w', 'n'};
    uint8_t response_ok_body[] = {0x00, 0x00, 0x00, 0x04, 0x1a, 0x02, 'h', 'i'};
    uint8_t response_unavailable[] = {0x00, 0x00, 0x00, 0x08, 0x08, 0x0e, 0x12, 0x04, 'd', 'o', 'w', 'n'};

    int err = trevrpc_wire_encode_request(
        "svc", "m", TREVRPC_RPC_KIND_UNARY, body, sizeof(body), NULL, 0, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(bytes_equal(frame, frame_len, request_unary, sizeof(request_unary)));
    free(frame);
    frame = NULL;

    err = trevrpc_wire_encode_request(
        "svc", "m", TREVRPC_RPC_KIND_UNARY, body, sizeof(body), NULL, 123456, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(bytes_equal(frame, frame_len, request_timeout, sizeof(request_timeout)));
    free(frame);
    frame = NULL;

    CHECK_GOTO(trevrpc_metadata_set(
                   &metadata, "authorization", strlen("authorization"), metadata_value, sizeof(metadata_value)) == 0);
    err = trevrpc_wire_encode_request(
        "svc", "m", TREVRPC_RPC_KIND_UNARY, body, sizeof(body), &metadata, 0, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(bytes_equal(frame, frame_len, request_metadata, sizeof(request_metadata)));
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
    CHECK_GOTO(bytes_equal(frame, frame_len, stream_message, sizeof(stream_message)));
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
    CHECK_GOTO(bytes_equal(frame, frame_len, stream_status, sizeof(stream_status)));
    free(frame);
    frame = NULL;

    CHECK_GOTO(trevrpc_response_set_body(&response, body, sizeof(body)) == 0);
    err = trevrpc_wire_encode_response(&response, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(bytes_equal(frame, frame_len, response_ok_body, sizeof(response_ok_body)));
    free(frame);
    frame = NULL;
    trevrpc_response_reset(&response);

    response.status = TREVRPC_STATUS_UNAVAILABLE;
    CHECK_GOTO(trevrpc_response_set_message(&response, "down", strlen("down")) == 0);
    err = trevrpc_wire_encode_response(&response, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(bytes_equal(frame, frame_len, response_unavailable, sizeof(response_unavailable)));

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
    if (test_wire_golden_vectors() != 0) {
        return 1;
    }
    return 0;
}
