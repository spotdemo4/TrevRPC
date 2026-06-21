#include "trevrpc.h"
#include "trevrpc_wire.h"

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

static int test_request_round_trip(void) {
    int result = 1;
    uint8_t body[] = {0xde, 0xad, 0xbe, 0xef};
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_request request;

    int err = trevrpc_wire_encode_request("test.EchoService",
        "Echo",
        TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING,
        body,
        sizeof(body),
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
    free(frame);
    return result;
}

static int test_request_rejects_bad_frames(void) {
    int result = 1;
    uint8_t* frame = NULL;
    size_t frame_len = 0;
    trevrpc_request request;
    uint8_t invalid_varint[] = {0x80};

    int err = trevrpc_wire_decode_request(invalid_varint, sizeof(invalid_varint), &request);
    CHECK_GOTO(err == TREVRPC_ERR_INVALID_FRAME);

    err = trevrpc_wire_encode_request("svc", "method", TREVRPC_RPC_KIND_UNARY, NULL, 0, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    CHECK_GOTO(frame_len > 4);
    frame[frame_len - 1] = 2;
    err = trevrpc_wire_decode_request(frame + 4, frame_len - 4, &request);
    CHECK_GOTO(err == TREVRPC_ERR_UNSUPPORTED_WIRE_VERSION);
    free(frame);
    frame = NULL;

    err = trevrpc_wire_encode_request("svc", "method", 99, NULL, 0, 1024, &frame, &frame_len);
    CHECK_GOTO(err == 0);
    err = trevrpc_wire_decode_request(frame + 4, frame_len - 4, &request);
    CHECK_GOTO(err == TREVRPC_ERR_UNSUPPORTED_RPC_KIND);

    result = 0;

cleanup:
    free(frame);
    return result;
}

static int test_encode_rejects_oversized_frames(void) {
    int result = 1;
    uint8_t body[] = {0x01, 0x02, 0x03, 0x04};
    uint8_t* frame = NULL;
    size_t frame_len = 0;

    int err =
        trevrpc_wire_encode_request("svc", "method", TREVRPC_RPC_KIND_UNARY, body, sizeof(body), 1, &frame, &frame_len);
    CHECK_GOTO(err == TREVRPC_ERR_FRAME_TOO_LARGE);
    CHECK_GOTO(frame == NULL);
    CHECK_GOTO(frame_len == 0);

    result = 0;

cleanup:
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
    if (test_response_round_trip() != 0) {
        return 1;
    }
    if (test_stream_frame_round_trip() != 0) {
        return 1;
    }
    return 0;
}
