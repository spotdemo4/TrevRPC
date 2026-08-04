#ifndef TREVRPC_FRAME_INTERNAL_H
#define TREVRPC_FRAME_INTERNAL_H

#include "trevrpc_owned_bytes_internal.h"

#include <stddef.h>
#include <stdint.h>

typedef void* (*trevrpc_frame_alloc_fn)(size_t size, void* context);
typedef trevrpc_owned_bytes_release_fn trevrpc_frame_free_fn;

void trevrpc_frame_default_free(void* ptr, void* context);

typedef enum trevrpc_frame_result {
    TREVRPC_FRAME_NEED_MORE = 0,
    TREVRPC_FRAME_READY = 1,
    TREVRPC_FRAME_CLEAN_EOF = 2,
    TREVRPC_FRAME_INCOMPLETE = 3,
    TREVRPC_FRAME_TOO_LARGE = 4,
    TREVRPC_FRAME_ALLOCATION_FAILURE = 5,
} trevrpc_frame_result;

typedef struct trevrpc_frame_parser {
    uint8_t header[4];
    size_t header_len;
    uint8_t* body;
    size_t body_len;
    size_t body_offset;
    size_t skip_remaining;
    size_t max_body_len;
    trevrpc_frame_alloc_fn alloc;
    trevrpc_frame_free_fn dealloc;
    void* allocator_context;
} trevrpc_frame_parser;

void trevrpc_frame_parser_init(trevrpc_frame_parser* parser, size_t max_body_len);
void trevrpc_frame_parser_init_with_allocator(trevrpc_frame_parser* parser,
    size_t max_body_len,
    trevrpc_frame_alloc_fn alloc,
    trevrpc_frame_free_fn dealloc,
    void* allocator_context);
void trevrpc_frame_parser_set_max_body_len(trevrpc_frame_parser* parser, size_t max_body_len);
trevrpc_frame_result trevrpc_frame_parser_consume(trevrpc_frame_parser* parser,
    const uint8_t* data,
    size_t len,
    size_t* consumed,
    uint8_t** body,
    size_t* body_len,
    size_t* declared_body_len);
trevrpc_frame_result trevrpc_frame_parser_consume_owned(trevrpc_frame_parser* parser,
    const uint8_t* data,
    size_t len,
    size_t* consumed,
    trevrpc_owned_bytes* body,
    size_t* declared_body_len);
trevrpc_frame_result trevrpc_frame_parser_finish(const trevrpc_frame_parser* parser);
void trevrpc_frame_parser_reset(trevrpc_frame_parser* parser);

#endif
