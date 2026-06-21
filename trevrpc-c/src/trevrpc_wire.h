#ifndef TREVRPC_WIRE_H
#define TREVRPC_WIRE_H

#include "trevrpc.h"

#include <stddef.h>
#include <stdint.h>

int trevrpc_wire_encode_request(const char* service,
    const char* method,
    uint32_t kind,
    const uint8_t* body,
    size_t body_len,
    size_t max_frame_size,
    uint8_t** frame,
    size_t* frame_len);
int trevrpc_wire_encode_response(
    const trevrpc_response* response, size_t max_frame_size, uint8_t** frame, size_t* frame_len);
int trevrpc_wire_encode_stream_frame(uint32_t kind,
    uint32_t status,
    const char* message,
    size_t message_len,
    const uint8_t* body,
    size_t body_len,
    size_t max_frame_size,
    uint8_t** frame,
    size_t* frame_len);

int trevrpc_wire_decode_request(const uint8_t* data, size_t len, trevrpc_request* request);
int trevrpc_wire_decode_response(const uint8_t* data, size_t len, trevrpc_response** out_response);
int trevrpc_wire_decode_stream_frame(const uint8_t* data, size_t len, trevrpc_stream_frame** out_frame);

#endif
