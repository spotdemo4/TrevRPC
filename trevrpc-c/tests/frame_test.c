#include "trevrpc_frame_internal.h"

#include <stdbool.h>
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

static trevrpc_frame_result consume(trevrpc_frame_parser* parser,
    const uint8_t* data,
    size_t len,
    size_t* consumed,
    uint8_t** body,
    size_t* body_len,
    size_t* declared_body_len) {
    return trevrpc_frame_parser_consume(parser, data, len, consumed, body, body_len, declared_body_len);
}

static int test_every_split(void) {
    static const uint8_t framed[] = {0x00, 0x00, 0x00, 0x03, 'a', 'b', 'c'};
    for (size_t split = 0; split <= sizeof(framed); split++) {
        trevrpc_frame_parser parser;
        trevrpc_frame_parser_init(&parser, 16);
        uint8_t* body = NULL;
        size_t body_len = 0;
        size_t declared = 0;
        size_t offset = 0;
        const size_t chunks[] = {split, sizeof(framed) - split};
        for (size_t chunk_index = 0; chunk_index < 2; chunk_index++) {
            size_t remaining = chunks[chunk_index];
            while (remaining > 0) {
                size_t consumed = 0;
                trevrpc_frame_result result =
                    consume(&parser, framed + offset, remaining, &consumed, &body, &body_len, &declared);
                CHECK(consumed > 0);
                offset += consumed;
                remaining -= consumed;
                if (result == TREVRPC_FRAME_READY) {
                    CHECK(body_len == 3);
                    CHECK(memcmp(body, "abc", 3) == 0);
                    free(body);
                    body = NULL;
                } else {
                    CHECK(result == TREVRPC_FRAME_NEED_MORE);
                }
            }
        }
        CHECK(offset == sizeof(framed));
        CHECK(trevrpc_frame_parser_finish(&parser) == TREVRPC_FRAME_CLEAN_EOF);
        trevrpc_frame_parser_reset(&parser);
    }
    return 0;
}

static int test_zero_length_and_multiple_frames(void) {
    static const uint8_t framed[] = {0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x7f};
    trevrpc_frame_parser parser;
    trevrpc_frame_parser_init(&parser, 1);
    size_t offset = 0;
    int ready_count = 0;
    while (offset < sizeof(framed)) {
        size_t consumed = 0;
        uint8_t* body = NULL;
        size_t body_len = 0;
        size_t declared = 0;
        trevrpc_frame_result result =
            consume(&parser, framed + offset, sizeof(framed) - offset, &consumed, &body, &body_len, &declared);
        CHECK(consumed > 0);
        offset += consumed;
        if (result == TREVRPC_FRAME_READY) {
            if (ready_count == 0) {
                CHECK(body == NULL);
                CHECK(body_len == 0);
            } else {
                CHECK(body_len == 1);
                CHECK(body[0] == 0x7f);
            }
            free(body);
            ready_count++;
        } else {
            CHECK(result == TREVRPC_FRAME_NEED_MORE);
        }
    }
    CHECK(ready_count == 2);
    CHECK(trevrpc_frame_parser_finish(&parser) == TREVRPC_FRAME_CLEAN_EOF);
    trevrpc_frame_parser_reset(&parser);
    return 0;
}

static int test_partial_fin(void) {
    static const uint8_t partial_header[] = {0x00, 0x00, 0x00};
    static const uint8_t partial_body[] = {0x00, 0x00, 0x00, 0x02, 0x01};
    const struct {
        const uint8_t* data;
        size_t len;
    } cases[] = {{partial_header, sizeof(partial_header)}, {partial_body, sizeof(partial_body)}};
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        trevrpc_frame_parser parser;
        trevrpc_frame_parser_init(&parser, 16);
        size_t consumed = 0;
        uint8_t* body = NULL;
        size_t body_len = 0;
        size_t declared = 0;
        CHECK(consume(&parser, cases[i].data, cases[i].len, &consumed, &body, &body_len, &declared) ==
              TREVRPC_FRAME_NEED_MORE);
        CHECK(consumed == cases[i].len);
        CHECK(trevrpc_frame_parser_finish(&parser) == TREVRPC_FRAME_INCOMPLETE);
        trevrpc_frame_parser_reset(&parser);
    }
    return 0;
}

static int test_oversize_skips_and_recovers(void) {
    static const uint8_t framed[] = {
        0x00,
        0x00,
        0x00,
        0x03,
        'x',
        'y',
        'z',
        0x00,
        0x00,
        0x00,
        0x01,
        'a',
    };
    trevrpc_frame_parser parser;
    trevrpc_frame_parser_init(&parser, 1);
    size_t offset = 0;
    bool saw_too_large = false;
    bool saw_ready = false;
    while (offset < sizeof(framed)) {
        size_t consumed = 0;
        uint8_t* body = NULL;
        size_t body_len = 0;
        size_t declared = 0;
        trevrpc_frame_result result =
            consume(&parser, framed + offset, sizeof(framed) - offset, &consumed, &body, &body_len, &declared);
        CHECK(consumed > 0);
        offset += consumed;
        if (result == TREVRPC_FRAME_TOO_LARGE) {
            CHECK(!saw_too_large);
            CHECK(declared == 3);
            saw_too_large = true;
        } else if (result == TREVRPC_FRAME_READY) {
            CHECK(body_len == 1);
            CHECK(body[0] == 'a');
            free(body);
            saw_ready = true;
        } else {
            CHECK(result == TREVRPC_FRAME_NEED_MORE);
        }
    }
    CHECK(saw_too_large);
    CHECK(saw_ready);
    CHECK(trevrpc_frame_parser_finish(&parser) == TREVRPC_FRAME_CLEAN_EOF);
    trevrpc_frame_parser_reset(&parser);
    return 0;
}

static void* fail_alloc(size_t size, void* context) {
    (void)size;
    (void)context;
    return NULL;
}

static void ignore_free(void* ptr, void* context) {
    (void)ptr;
    (void)context;
}

static int test_allocation_failure(void) {
    static const uint8_t header[] = {0x00, 0x00, 0x00, 0x01};
    trevrpc_frame_parser parser;
    trevrpc_frame_parser_init_with_allocator(&parser, 1, fail_alloc, ignore_free, NULL);
    size_t consumed = 0;
    uint8_t* body = NULL;
    size_t body_len = 0;
    size_t declared = 0;
    CHECK(consume(&parser, header, sizeof(header), &consumed, &body, &body_len, &declared) ==
          TREVRPC_FRAME_ALLOCATION_FAILURE);
    CHECK(consumed == sizeof(header));
    CHECK(declared == 1);
    CHECK(trevrpc_frame_parser_finish(&parser) == TREVRPC_FRAME_CLEAN_EOF);
    trevrpc_frame_parser_reset(&parser);
    return 0;
}

typedef struct allocator_counter {
    size_t allocations;
    size_t releases;
} allocator_counter;

static void* counted_alloc(size_t size, void* context) {
    allocator_counter* counter = context;
    counter->allocations++;
    return malloc(size);
}

static void counted_free(void* ptr, void* context) {
    allocator_counter* counter = context;
    counter->releases++;
    free(ptr);
}

static int test_owned_ready_transfer_and_partial_reset(void) {
    static const uint8_t framed[] = {0x00, 0x00, 0x00, 0x03, 'a', 'b', 'c'};
    allocator_counter counter = {0};
    trevrpc_frame_parser parser;
    trevrpc_frame_parser_init_with_allocator(&parser, 16, counted_alloc, counted_free, &counter);
    trevrpc_owned_bytes body = {0};
    size_t consumed = 0;
    size_t declared = 0;
    CHECK(trevrpc_frame_parser_consume_owned(&parser, framed, sizeof(framed), &consumed, &body, &declared) ==
          TREVRPC_FRAME_READY);
    CHECK(consumed == sizeof(framed));
    CHECK(declared == 3);
    CHECK(body.owner != NULL && body.data == body.owner && body.len == 3);
    CHECK(counter.allocations == 1 && counter.releases == 0);
    trevrpc_frame_parser_reset(&parser);
    CHECK(counter.releases == 0);
    trevrpc_owned_bytes_reset(&body);
    CHECK(counter.releases == 1);

    trevrpc_frame_parser_init_with_allocator(&parser, 16, counted_alloc, counted_free, &counter);
    consumed = 0;
    CHECK(
        trevrpc_frame_parser_consume_owned(&parser, framed, 5, &consumed, &body, &declared) == TREVRPC_FRAME_NEED_MORE);
    CHECK(counter.allocations == 2 && counter.releases == 1);
    trevrpc_frame_parser_reset(&parser);
    CHECK(counter.releases == 2);
    return 0;
}

static int test_owned_oversize_does_not_allocate(void) {
    static const uint8_t framed[] = {0x00, 0x00, 0x00, 0x02, 'a', 'b'};
    allocator_counter counter = {0};
    trevrpc_frame_parser parser;
    trevrpc_frame_parser_init_with_allocator(&parser, 1, counted_alloc, counted_free, &counter);
    trevrpc_owned_bytes body = {0};
    size_t consumed = 0;
    size_t declared = 0;
    CHECK(trevrpc_frame_parser_consume_owned(&parser, framed, sizeof(framed), &consumed, &body, &declared) ==
          TREVRPC_FRAME_TOO_LARGE);
    CHECK(consumed == 4 && declared == 2);
    CHECK(counter.allocations == 0 && counter.releases == 0 && body.owner == NULL);
    trevrpc_frame_parser_reset(&parser);
    CHECK(counter.releases == 0);
    return 0;
}

int main(void) {
    if (test_every_split() != 0 || test_zero_length_and_multiple_frames() != 0 || test_partial_fin() != 0 ||
        test_oversize_skips_and_recovers() != 0 || test_allocation_failure() != 0 ||
        test_owned_ready_transfer_and_partial_reset() != 0 || test_owned_oversize_does_not_allocate() != 0) {
        return 1;
    }
    return 0;
}
