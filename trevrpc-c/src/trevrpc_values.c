#include "trevrpc.h"

#include <errno.h>
#include <stdlib.h>
#include <string.h>

static int trevrpc_copy_chars(char** dst, size_t* dst_len, const char* src, size_t src_len) {
    if (src == NULL && src_len > 0) {
        return -EINVAL;
    }

    char* copy = NULL;
    if (src_len > 0) {
        copy = malloc(src_len);
        if (copy == NULL) {
            return -ENOMEM;
        }
        memcpy(copy, src, src_len);
    }

    free(*dst);
    *dst = copy;
    *dst_len = src_len;
    return 0;
}

static int trevrpc_copy_bytes(uint8_t** dst, size_t* dst_len, const uint8_t* src, size_t src_len) {
    if (src == NULL && src_len > 0) {
        return -EINVAL;
    }

    uint8_t* copy = NULL;
    if (src_len > 0) {
        copy = malloc(src_len);
        if (copy == NULL) {
            return -ENOMEM;
        }
        memcpy(copy, src, src_len);
    }

    free(*dst);
    *dst = copy;
    *dst_len = src_len;
    return 0;
}

int trevrpc_response_set_message(trevrpc_response* response, const char* message, size_t message_len) {
    if (response == NULL) {
        return -EINVAL;
    }

    return trevrpc_copy_chars(&response->message, &response->message_len, message, message_len);
}

int trevrpc_response_set_body(trevrpc_response* response, const uint8_t* body, size_t body_len) {
    if (response == NULL) {
        return -EINVAL;
    }

    return trevrpc_copy_bytes(&response->body, &response->body_len, body, body_len);
}

void trevrpc_response_reset(trevrpc_response* response) {
    if (response == NULL) {
        return;
    }

    free(response->message);
    free(response->body);
    response->status = TREVRPC_STATUS_OK;
    response->message = NULL;
    response->message_len = 0;
    response->body = NULL;
    response->body_len = 0;
}

void trevrpc_response_free(trevrpc_response* response) {
    if (response == NULL) {
        return;
    }

    trevrpc_response_reset(response);
    free(response);
}

int trevrpc_stream_frame_set_message(trevrpc_stream_frame* frame, const char* message, size_t message_len) {
    if (frame == NULL) {
        return -EINVAL;
    }

    return trevrpc_copy_chars(&frame->message, &frame->message_len, message, message_len);
}

int trevrpc_stream_frame_set_body(trevrpc_stream_frame* frame, const uint8_t* body, size_t body_len) {
    if (frame == NULL) {
        return -EINVAL;
    }

    return trevrpc_copy_bytes(&frame->body, &frame->body_len, body, body_len);
}

void trevrpc_stream_frame_reset(trevrpc_stream_frame* frame) {
    if (frame == NULL) {
        return;
    }

    free(frame->message);
    free(frame->body);
    frame->kind = TREVRPC_STREAM_FRAME_KIND_MESSAGE;
    frame->status = TREVRPC_STATUS_OK;
    frame->message = NULL;
    frame->message_len = 0;
    frame->body = NULL;
    frame->body_len = 0;
}

void trevrpc_stream_frame_free(trevrpc_stream_frame* frame) {
    if (frame == NULL) {
        return;
    }

    trevrpc_stream_frame_reset(frame);
    free(frame);
}
