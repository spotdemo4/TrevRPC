#include "trevrpc_owned_bytes_internal.h"

#include <errno.h> // IWYU pragma: keep
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

struct trevrpc_body_owner {
    trevrpc_owned_bytes body;
};

struct trevrpc_inbound_response {
    uint32_t status;
    char* message;
    size_t message_len;
    trevrpc_metadata metadata;
    trevrpc_owned_bytes body;
    bool body_taken;
};

struct trevrpc_inbound_stream_frame {
    uint32_t kind;
    uint32_t status;
    char* message;
    size_t message_len;
    trevrpc_metadata metadata;
    trevrpc_owned_bytes body;
    bool body_taken;
};

static int trevrpc_body_owner_take(trevrpc_owned_bytes* body, bool* taken, trevrpc_body_owner** out_owner) {
    if (body == NULL || taken == NULL || out_owner == NULL) {
        return -EINVAL;
    }
    *out_owner = NULL;
    if (*taken) {
        return -EALREADY;
    }

    trevrpc_body_owner* owner = NULL;
    if (body->owner != NULL || body->release != NULL) {
        owner = malloc(sizeof(*owner));
        if (owner == NULL) {
            return -ENOMEM;
        }
        trevrpc_owned_bytes_init(&owner->body);
    }

    *taken = true;
    if (owner != NULL) {
        trevrpc_owned_bytes_move(&owner->body, body);
        *out_owner = owner;
    } else {
        trevrpc_owned_bytes_init(body);
    }
    return 0;
}

int trevrpc_body_owner_get_view(const trevrpc_body_owner* owner, trevrpc_bytes_view* body) {
    if (owner == NULL || body == NULL) {
        return -EINVAL;
    }
    body->data = owner->body.data;
    body->len = owner->body.len;
    return 0;
}

void trevrpc_body_owner_release(trevrpc_body_owner* owner) {
    if (owner == NULL) {
        return;
    }
    trevrpc_owned_bytes_reset(&owner->body);
    free(owner);
}

int trevrpc_inbound_response_create(trevrpc_wire_response_values* values, trevrpc_inbound_response** inbound) {
    if (values == NULL || inbound == NULL) {
        return -EINVAL;
    }
    *inbound = NULL;
    trevrpc_inbound_response* created = calloc(1, sizeof(*created));
    if (created == NULL) {
        return -ENOMEM;
    }
    created->status = values->status;
    created->message = values->message;
    created->message_len = values->message_len;
    created->metadata = values->metadata;
    trevrpc_owned_bytes_move(&created->body, &values->body);
    values->message = NULL;
    values->message_len = 0;
    values->metadata.entries = NULL;
    values->metadata.entries_len = 0;
    *inbound = created;
    return 0;
}

int trevrpc_inbound_stream_frame_create(
    trevrpc_wire_stream_frame_values* values, trevrpc_inbound_stream_frame** inbound) {
    if (values == NULL || inbound == NULL) {
        return -EINVAL;
    }
    *inbound = NULL;
    trevrpc_inbound_stream_frame* created = calloc(1, sizeof(*created));
    if (created == NULL) {
        return -ENOMEM;
    }
    created->kind = values->kind;
    created->status = values->status;
    created->message = values->message;
    created->message_len = values->message_len;
    created->metadata = values->metadata;
    trevrpc_owned_bytes_move(&created->body, &values->body);
    values->message = NULL;
    values->message_len = 0;
    values->metadata.entries = NULL;
    values->metadata.entries_len = 0;
    *inbound = created;
    return 0;
}

int trevrpc_inbound_response_get_status(const trevrpc_inbound_response* response, uint32_t* status) {
    if (response == NULL || status == NULL) {
        return -EINVAL;
    }
    *status = response->status;
    return 0;
}

int trevrpc_inbound_response_get_message(const trevrpc_inbound_response* response, trevrpc_bytes_view* message) {
    if (response == NULL || message == NULL) {
        return -EINVAL;
    }
    message->data = (const uint8_t*)response->message;
    message->len = response->message_len;
    return 0;
}

int trevrpc_inbound_response_get_body(const trevrpc_inbound_response* response, trevrpc_bytes_view* body) {
    if (response == NULL || body == NULL) {
        return -EINVAL;
    }
    if (response->body_taken) {
        return -EALREADY;
    }
    body->data = response->body.data;
    body->len = response->body.len;
    return 0;
}

size_t trevrpc_inbound_response_metadata_count(const trevrpc_inbound_response* response) {
    return response == NULL ? 0 : response->metadata.entries_len;
}

int trevrpc_inbound_response_metadata_at(
    const trevrpc_inbound_response* response, size_t index, trevrpc_bytes_view* key, trevrpc_bytes_view* value) {
    if (response == NULL || key == NULL || value == NULL || index >= response->metadata.entries_len) {
        return -EINVAL;
    }
    const trevrpc_metadata_entry* entry = &response->metadata.entries[index];
    key->data = (const uint8_t*)entry->key;
    key->len = entry->key_len;
    value->data = entry->value;
    value->len = entry->value_len;
    return 0;
}

int trevrpc_inbound_response_take_body(trevrpc_inbound_response* response, trevrpc_body_owner** owner) {
    if (response == NULL || owner == NULL) {
        return -EINVAL;
    }
    return trevrpc_body_owner_take(&response->body, &response->body_taken, owner);
}

void trevrpc_inbound_response_release(trevrpc_inbound_response* response) {
    if (response == NULL) {
        return;
    }
    free(response->message);
    trevrpc_metadata_reset(&response->metadata);
    trevrpc_owned_bytes_reset(&response->body);
    free(response);
}

int trevrpc_inbound_stream_frame_get_kind(const trevrpc_inbound_stream_frame* frame, uint32_t* kind) {
    if (frame == NULL || kind == NULL) {
        return -EINVAL;
    }
    *kind = frame->kind;
    return 0;
}

int trevrpc_inbound_stream_frame_get_status(const trevrpc_inbound_stream_frame* frame, uint32_t* status) {
    if (frame == NULL || status == NULL) {
        return -EINVAL;
    }
    *status = frame->status;
    return 0;
}

int trevrpc_inbound_stream_frame_get_message(const trevrpc_inbound_stream_frame* frame, trevrpc_bytes_view* message) {
    if (frame == NULL || message == NULL) {
        return -EINVAL;
    }
    message->data = (const uint8_t*)frame->message;
    message->len = frame->message_len;
    return 0;
}

int trevrpc_inbound_stream_frame_get_body(const trevrpc_inbound_stream_frame* frame, trevrpc_bytes_view* body) {
    if (frame == NULL || body == NULL) {
        return -EINVAL;
    }
    if (frame->body_taken) {
        return -EALREADY;
    }
    body->data = frame->body.data;
    body->len = frame->body.len;
    return 0;
}

size_t trevrpc_inbound_stream_frame_metadata_count(const trevrpc_inbound_stream_frame* frame) {
    return frame == NULL ? 0 : frame->metadata.entries_len;
}

int trevrpc_inbound_stream_frame_metadata_at(
    const trevrpc_inbound_stream_frame* frame, size_t index, trevrpc_bytes_view* key, trevrpc_bytes_view* value) {
    if (frame == NULL || key == NULL || value == NULL || index >= frame->metadata.entries_len) {
        return -EINVAL;
    }
    const trevrpc_metadata_entry* entry = &frame->metadata.entries[index];
    key->data = (const uint8_t*)entry->key;
    key->len = entry->key_len;
    value->data = entry->value;
    value->len = entry->value_len;
    return 0;
}

int trevrpc_inbound_stream_frame_take_body(trevrpc_inbound_stream_frame* frame, trevrpc_body_owner** owner) {
    if (frame == NULL || owner == NULL) {
        return -EINVAL;
    }
    return trevrpc_body_owner_take(&frame->body, &frame->body_taken, owner);
}

void trevrpc_inbound_stream_frame_release(trevrpc_inbound_stream_frame* frame) {
    if (frame == NULL) {
        return;
    }
    free(frame->message);
    trevrpc_metadata_reset(&frame->metadata);
    trevrpc_owned_bytes_reset(&frame->body);
    free(frame);
}
