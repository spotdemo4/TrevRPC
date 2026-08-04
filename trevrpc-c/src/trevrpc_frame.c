#include "trevrpc_frame_internal.h"

#include <stdlib.h>
#include <string.h>

static void* trevrpc_frame_default_alloc(size_t size, void* context) {
    (void)context;
    return malloc(size);
}

void trevrpc_frame_default_free(void* ptr, void* context) {
    (void)context;
    free(ptr);
}

static size_t trevrpc_frame_header_body_len(const uint8_t header[4]) {
    return ((size_t)header[0] << 24) | ((size_t)header[1] << 16) | ((size_t)header[2] << 8) | (size_t)header[3];
}

void trevrpc_frame_parser_init(trevrpc_frame_parser* parser, size_t max_body_len) {
    trevrpc_frame_parser_init_with_allocator(
        parser, max_body_len, trevrpc_frame_default_alloc, trevrpc_frame_default_free, NULL);
}

void trevrpc_frame_parser_init_with_allocator(trevrpc_frame_parser* parser,
    size_t max_body_len,
    trevrpc_frame_alloc_fn alloc,
    trevrpc_frame_free_fn dealloc,
    void* allocator_context) {
    if (parser == NULL) {
        return;
    }
    memset(parser, 0, sizeof(*parser));
    parser->max_body_len = max_body_len;
    parser->alloc = alloc == NULL ? trevrpc_frame_default_alloc : alloc;
    parser->dealloc = dealloc == NULL ? trevrpc_frame_default_free : dealloc;
    parser->allocator_context = allocator_context;
}

void trevrpc_frame_parser_set_max_body_len(trevrpc_frame_parser* parser, size_t max_body_len) {
    if (parser != NULL) {
        parser->max_body_len = max_body_len;
    }
}

static void trevrpc_frame_parser_reset_message(trevrpc_frame_parser* parser) {
    parser->header_len = 0;
    parser->body = NULL;
    parser->body_len = 0;
    parser->body_offset = 0;
}

trevrpc_frame_result trevrpc_frame_parser_consume(trevrpc_frame_parser* parser,
    const uint8_t* data,
    size_t len,
    size_t* consumed,
    uint8_t** body,
    size_t* body_len,
    size_t* declared_body_len) {
    if (consumed != NULL) {
        *consumed = 0;
    }
    if (body != NULL) {
        *body = NULL;
    }
    if (body_len != NULL) {
        *body_len = 0;
    }
    if (declared_body_len != NULL) {
        *declared_body_len = 0;
    }
    if (parser == NULL || consumed == NULL || body == NULL || body_len == NULL || declared_body_len == NULL ||
        (data == NULL && len > 0)) {
        return TREVRPC_FRAME_INCOMPLETE;
    }

    if (parser->skip_remaining > 0) {
        size_t skipped = len < parser->skip_remaining ? len : parser->skip_remaining;
        parser->skip_remaining -= skipped;
        *consumed = skipped;
        return TREVRPC_FRAME_NEED_MORE;
    }

    if (parser->header_len < sizeof(parser->header)) {
        size_t header_remaining = sizeof(parser->header) - parser->header_len;
        size_t copied = len < header_remaining ? len : header_remaining;
        if (copied > 0) {
            memcpy(parser->header + parser->header_len, data, copied);
            parser->header_len += copied;
            *consumed = copied;
        }
        if (parser->header_len < sizeof(parser->header)) {
            return TREVRPC_FRAME_NEED_MORE;
        }

        parser->body_len = trevrpc_frame_header_body_len(parser->header);
        *declared_body_len = parser->body_len;
        if (parser->body_len > parser->max_body_len) {
            parser->skip_remaining = parser->body_len;
            parser->header_len = 0;
            parser->body_len = 0;
            return TREVRPC_FRAME_TOO_LARGE;
        }
        if (parser->body_len == 0) {
            trevrpc_frame_parser_reset_message(parser);
            return TREVRPC_FRAME_READY;
        }
        parser->body = parser->alloc(parser->body_len, parser->allocator_context);
        if (parser->body == NULL) {
            trevrpc_frame_parser_reset_message(parser);
            return TREVRPC_FRAME_ALLOCATION_FAILURE;
        }
    }

    size_t remaining_input = len - *consumed;
    size_t body_remaining = parser->body_len - parser->body_offset;
    size_t copied = remaining_input < body_remaining ? remaining_input : body_remaining;
    if (copied > 0) {
        memcpy(parser->body + parser->body_offset, data + *consumed, copied);
        parser->body_offset += copied;
        *consumed += copied;
    }
    if (parser->body_offset < parser->body_len) {
        return TREVRPC_FRAME_NEED_MORE;
    }

    *body = parser->body;
    *body_len = parser->body_len;
    *declared_body_len = parser->body_len;
    trevrpc_frame_parser_reset_message(parser);
    return TREVRPC_FRAME_READY;
}

trevrpc_frame_result trevrpc_frame_parser_consume_owned(trevrpc_frame_parser* parser,
    const uint8_t* data,
    size_t len,
    size_t* consumed,
    trevrpc_owned_bytes* body,
    size_t* declared_body_len) {
    if (body == NULL) {
        return TREVRPC_FRAME_INCOMPLETE;
    }
    trevrpc_owned_bytes_init(body);
    uint8_t* raw_body = NULL;
    size_t raw_body_len = 0;
    trevrpc_frame_result result =
        trevrpc_frame_parser_consume(parser, data, len, consumed, &raw_body, &raw_body_len, declared_body_len);
    if (result == TREVRPC_FRAME_READY) {
        body->data = raw_body;
        body->len = raw_body_len;
        body->owner = raw_body;
        body->release = parser->dealloc;
        body->release_context = parser->allocator_context;
    }
    return result;
}

trevrpc_frame_result trevrpc_frame_parser_finish(const trevrpc_frame_parser* parser) {
    if (parser == NULL) {
        return TREVRPC_FRAME_INCOMPLETE;
    }
    return parser->header_len == 0 && parser->body == NULL && parser->skip_remaining == 0 ? TREVRPC_FRAME_CLEAN_EOF
                                                                                          : TREVRPC_FRAME_INCOMPLETE;
}

void trevrpc_frame_parser_reset(trevrpc_frame_parser* parser) {
    if (parser == NULL) {
        return;
    }
    if (parser->body != NULL) {
        parser->dealloc(parser->body, parser->allocator_context);
    }
    size_t max_body_len = parser->max_body_len;
    trevrpc_frame_alloc_fn alloc = parser->alloc;
    trevrpc_frame_free_fn dealloc = parser->dealloc;
    void* allocator_context = parser->allocator_context;
    memset(parser, 0, sizeof(*parser));
    parser->max_body_len = max_body_len;
    parser->alloc = alloc == NULL ? trevrpc_frame_default_alloc : alloc;
    parser->dealloc = dealloc == NULL ? trevrpc_frame_default_free : dealloc;
    parser->allocator_context = allocator_context;
}
