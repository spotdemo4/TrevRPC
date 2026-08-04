#include "trevrpc_owned_bytes_internal.h"
#include "trevrpc_wire_internal.h"

#include <errno.h> // IWYU pragma: keep
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define CHECK(condition)                                                                                               \
    do {                                                                                                               \
        if (!(condition)) {                                                                                            \
            fprintf(stderr, "%s:%d: check failed: %s\n", __FILE__, __LINE__, #condition);                              \
            return 1;                                                                                                  \
        }                                                                                                              \
    } while (0)

typedef struct release_counter {
    size_t calls;
    void* expected_owner;
    void* expected_context;
} release_counter;

static void counted_release(void* owner, void* context) {
    release_counter* counter = context;
    counter->calls++;
    if (owner != counter->expected_owner || context != counter->expected_context) {
        abort();
    }
    free(owner);
}

static int test_response_interior_take(void) {
    trevrpc_wire_response_values values = {0};
    CHECK(trevrpc_internal_response_set_message(&values, "ok", 2) == 0);
    const uint8_t metadata_value[] = {1, 2, 3};
    CHECK(trevrpc_metadata_set(&values.metadata, "key", 3, metadata_value, sizeof(metadata_value)) == 0);

    uint8_t* allocation = malloc(16);
    if (allocation == NULL) {
        trevrpc_internal_response_reset(&values);
        return 1;
    }
    memcpy(allocation + 5, "body", 4);
    release_counter counter = {.expected_owner = allocation};
    counter.expected_context = &counter;
    values.status = TREVRPC_STATUS_UNAVAILABLE;
    values.body = (trevrpc_owned_bytes){
        .data = allocation + 5,
        .len = 4,
        .owner = allocation,
        .release = counted_release,
        .release_context = &counter,
    };
    trevrpc_inbound_response* response = NULL;
    int create_err = trevrpc_inbound_response_create(&values, &response);
    if (create_err != 0) {
        trevrpc_owned_bytes_reset(&values.body);
        trevrpc_internal_response_reset(&values);
        return 1;
    }
    CHECK(values.body.owner == NULL);

    uint32_t status = 0;
    trevrpc_bytes_view view = {0};
    CHECK(trevrpc_inbound_response_get_status(response, &status) == 0);
    CHECK(status == TREVRPC_STATUS_UNAVAILABLE);
    CHECK(trevrpc_inbound_response_get_message(response, &view) == 0);
    CHECK(view.len == 2 && memcmp(view.data, "ok", 2) == 0);
    CHECK(trevrpc_inbound_response_get_body(response, &view) == 0);
    CHECK(view.data == allocation + 5 && view.len == 4);
    CHECK(trevrpc_inbound_response_metadata_count(response) == 1);
    trevrpc_bytes_view key = {0};
    trevrpc_bytes_view value = {0};
    CHECK(trevrpc_inbound_response_metadata_at(response, 0, &key, &value) == 0);
    CHECK(key.len == 3 && memcmp(key.data, "key", 3) == 0);
    CHECK(value.len == sizeof(metadata_value) && memcmp(value.data, metadata_value, sizeof(metadata_value)) == 0);

    trevrpc_body_owner* owner = NULL;
    CHECK(trevrpc_inbound_response_take_body(response, &owner) == 0);
    CHECK(owner != NULL);
    trevrpc_body_owner* second_owner = (trevrpc_body_owner*)1;
    CHECK(trevrpc_inbound_response_take_body(response, &second_owner) == -EALREADY);
    CHECK(second_owner == NULL);
    CHECK(trevrpc_inbound_response_get_body(response, &view) == -EALREADY);
    CHECK(trevrpc_body_owner_get_view(owner, &view) == 0);
    CHECK(view.data == allocation + 5 && view.len == 4);

    trevrpc_inbound_response_release(response);
    CHECK(counter.calls == 0);
    trevrpc_body_owner_release(owner);
    CHECK(counter.calls == 1);
    trevrpc_internal_response_reset(&values);
    return 0;
}

static int test_zero_length_owner_and_release_paths(void) {
    uint8_t* allocation = malloc(1);
    CHECK(allocation != NULL);
    release_counter counter = {.expected_owner = allocation};
    counter.expected_context = &counter;
    trevrpc_wire_stream_frame_values values = {
        .body = {.owner = allocation, .release = counted_release, .release_context = &counter},
    };
    trevrpc_inbound_stream_frame* frame = NULL;
    CHECK(trevrpc_inbound_stream_frame_create(&values, &frame) == 0);
    trevrpc_body_owner* owner = NULL;
    CHECK(trevrpc_inbound_stream_frame_take_body(frame, &owner) == 0);
    CHECK(owner != NULL);
    trevrpc_bytes_view view = {(const uint8_t*)1, 1};
    CHECK(trevrpc_body_owner_get_view(owner, &view) == 0);
    CHECK(view.data == NULL && view.len == 0);
    trevrpc_inbound_stream_frame_release(frame);
    CHECK(counter.calls == 0);
    trevrpc_body_owner_release(owner);
    CHECK(counter.calls == 1);

    counter.calls = 0;
    counter.expected_owner = NULL;
    values = (trevrpc_wire_stream_frame_values){
        .body = {.release = counted_release, .release_context = &counter},
    };
    CHECK(trevrpc_inbound_stream_frame_create(&values, &frame) == 0);
    owner = NULL;
    CHECK(trevrpc_inbound_stream_frame_take_body(frame, &owner) == 0);
    CHECK(owner != NULL);
    trevrpc_inbound_stream_frame_release(frame);
    CHECK(counter.calls == 0);
    trevrpc_body_owner_release(owner);
    CHECK(counter.calls == 1);

    trevrpc_wire_response_values empty_values = {0};
    trevrpc_inbound_response* empty = NULL;
    CHECK(trevrpc_inbound_response_create(&empty_values, &empty) == 0);
    owner = (trevrpc_body_owner*)1;
    CHECK(trevrpc_inbound_response_take_body(empty, &owner) == 0);
    CHECK(owner == NULL);
    CHECK(trevrpc_inbound_response_take_body(empty, &owner) == -EALREADY);
    trevrpc_inbound_response_release(empty);

    allocation = malloc(1);
    CHECK(allocation != NULL);
    counter.calls = 0;
    counter.expected_owner = allocation;
    values = (trevrpc_wire_stream_frame_values){
        .body = {.owner = allocation, .release = counted_release, .release_context = &counter},
    };
    CHECK(trevrpc_inbound_stream_frame_create(&values, &frame) == 0);
    trevrpc_inbound_stream_frame_release(frame);
    CHECK(counter.calls == 1);
    return 0;
}

static int test_owned_wire_final_body_and_failure_release(void) {
    uint8_t* allocation = malloc(16);
    CHECK(allocation != NULL);
    const uint8_t encoded[] = {0x1a, 0x03, 'o', 'l', 'd', 0x1a, 0x03, 'n', 'e', 'w'};
    memcpy(allocation + 2, encoded, sizeof(encoded));
    release_counter counter = {.expected_owner = allocation};
    counter.expected_context = &counter;
    trevrpc_owned_bytes owned = {
        .data = allocation + 2,
        .len = sizeof(encoded),
        .owner = allocation,
        .release = counted_release,
        .release_context = &counter,
    };
    trevrpc_inbound_response* response = NULL;
    CHECK(trevrpc_wire_decode_response_owned(&owned, &response) == 0);
    CHECK(owned.owner == NULL);
    trevrpc_bytes_view body = {0};
    CHECK(trevrpc_inbound_response_get_body(response, &body) == 0);
    CHECK(body.len == 3 && memcmp(body.data, "new", 3) == 0);
    CHECK(body.data == allocation + 9);
    trevrpc_inbound_response_release(response);
    CHECK(counter.calls == 1);

    allocation = malloc(2);
    CHECK(allocation != NULL);
    allocation[0] = 0x1a;
    allocation[1] = 0x80;
    counter.calls = 0;
    counter.expected_owner = allocation;
    owned = (trevrpc_owned_bytes){
        .data = allocation,
        .len = 2,
        .owner = allocation,
        .release = counted_release,
        .release_context = &counter,
    };
    CHECK(trevrpc_wire_decode_response_owned(&owned, &response) == TREVRPC_ERR_INVALID_FRAME);
    CHECK(response == NULL && owned.owner == NULL && counter.calls == 1);
    return 0;
}

int main(void) {
    if (test_response_interior_take() != 0 || test_zero_length_owner_and_release_paths() != 0 ||
        test_owned_wire_final_body_and_failure_release() != 0) {
        return 1;
    }
    trevrpc_inbound_response_release(NULL);
    trevrpc_inbound_stream_frame_release(NULL);
    trevrpc_body_owner_release(NULL);
    return 0;
}
