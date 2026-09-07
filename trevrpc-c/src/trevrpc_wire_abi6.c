#include "trevrpc_wire_abi6_internal.h"

#include <errno.h> // IWYU pragma: keep

int trevrpc_wire_decode_response_owned(trevrpc_owned_bytes* data, trevrpc_inbound_response** out_response) {
    if (data == NULL) {
        return -EINVAL;
    }
    trevrpc_owned_bytes owned = {0};
    trevrpc_owned_bytes_move(&owned, data);
    if (out_response == NULL || (owned.data == NULL && owned.len > 0)) {
        trevrpc_owned_bytes_reset(&owned);
        return -EINVAL;
    }
    *out_response = NULL;

    const uint8_t* visible_body = NULL;
    size_t visible_body_len = 0;
    trevrpc_wire_response_values* values = NULL;
    trevrpc_wire_diagnostic diagnostic = {0};
    int err = trevrpc_internal_wire_decode_response_values(
        owned.data, owned.len, &values, NULL, NULL, &visible_body, &visible_body_len, &diagnostic);
    if (err != 0) {
        trevrpc_owned_bytes_reset(&owned);
        return err;
    }
    owned.data = visible_body;
    owned.len = visible_body_len;
    trevrpc_owned_bytes_move(&values->body, &owned);
    err = trevrpc_inbound_response_create(values, out_response);
    trevrpc_internal_response_free(values);
    if (err != 0) {
        trevrpc_owned_bytes_reset(&owned);
    }
    return err;
}

int trevrpc_wire_decode_stream_frame_owned_diagnostic(
    trevrpc_owned_bytes* data, trevrpc_inbound_stream_frame** out_frame, trevrpc_wire_diagnostic* diagnostic) {
    if (data == NULL) {
        return -EINVAL;
    }
    trevrpc_owned_bytes owned = {0};
    trevrpc_owned_bytes_move(&owned, data);
    if (out_frame == NULL || diagnostic == NULL || (owned.data == NULL && owned.len > 0)) {
        trevrpc_owned_bytes_reset(&owned);
        return -EINVAL;
    }
    *out_frame = NULL;

    const uint8_t* visible_body = NULL;
    size_t visible_body_len = 0;
    trevrpc_wire_stream_frame_values* values = NULL;
    int err = trevrpc_internal_wire_decode_stream_frame_values(
        owned.data, owned.len, &values, &visible_body, &visible_body_len, diagnostic);
    if (err != 0) {
        trevrpc_owned_bytes_reset(&owned);
        return err;
    }
    owned.data = visible_body;
    owned.len = visible_body_len;
    trevrpc_owned_bytes_move(&values->body, &owned);
    err = trevrpc_inbound_stream_frame_create(values, out_frame);
    trevrpc_internal_stream_frame_free(values);
    if (err != 0) {
        trevrpc_owned_bytes_reset(&owned);
    }
    return err;
}

int trevrpc_wire_decode_stream_frame_owned(trevrpc_owned_bytes* data, trevrpc_inbound_stream_frame** out_frame) {
    trevrpc_wire_diagnostic diagnostic = {0};
    return trevrpc_wire_decode_stream_frame_owned_diagnostic(data, out_frame, &diagnostic);
}
