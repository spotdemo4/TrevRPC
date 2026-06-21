#include "trevrpc_webtransport.h"

#include <errno.h>
#include <stdlib.h>

struct trevrpc_wt_listener {
    int closed;
};

struct trevrpc_wt_session {
    int closed;
};

struct trevrpc_wt_stream {
    int closed;
};

static int trevrpc_wt_unsupported(void) {
    return TREV_WT_ERR_REJECTED;
}

int trevrpc_wt_listen(const trevrpc_wt_config* config, trevrpc_wt_listener** out_listener) {
    if (config == NULL || out_listener == NULL) {
        return -EINVAL;
    }
    *out_listener = NULL;
    return trevrpc_wt_unsupported();
}

int trevrpc_wt_listener_accept_session(trevrpc_wt_listener* listener, trevrpc_wt_session** out_session) {
    if (listener == NULL || out_session == NULL) {
        return -EINVAL;
    }
    *out_session = NULL;
    return listener->closed ? TREV_WT_ERR_CLOSED : trevrpc_wt_unsupported();
}

void trevrpc_wt_listener_shutdown(trevrpc_wt_listener* listener) {
    if (listener != NULL) {
        listener->closed = 1;
    }
}

void trevrpc_wt_listener_close(trevrpc_wt_listener* listener) {
    free(listener);
}

int trevrpc_wt_dial(const trevrpc_wt_config* config, trevrpc_wt_session** out_session) {
    if (config == NULL || out_session == NULL) {
        return -EINVAL;
    }
    *out_session = NULL;
    return trevrpc_wt_unsupported();
}

int trevrpc_wt_session_accept_stream(trevrpc_wt_session* session, trevrpc_wt_stream** out_stream) {
    if (session == NULL || out_stream == NULL) {
        return -EINVAL;
    }
    *out_stream = NULL;
    return session->closed ? TREV_WT_ERR_CLOSED : trevrpc_wt_unsupported();
}

int trevrpc_wt_session_open_stream(trevrpc_wt_session* session, trevrpc_wt_stream** out_stream) {
    if (session == NULL || out_stream == NULL) {
        return -EINVAL;
    }
    *out_stream = NULL;
    return session->closed ? TREV_WT_ERR_CLOSED : trevrpc_wt_unsupported();
}

void trevrpc_wt_session_close(trevrpc_wt_session* session) {
    if (session != NULL) {
        session->closed = 1;
        free(session);
    }
}

intptr_t trevrpc_wt_stream_read(trevrpc_wt_stream* stream, uint8_t* data, size_t len) {
    (void)data;
    (void)len;
    if (stream == NULL) {
        return -EINVAL;
    }
    return stream->closed ? TREV_WT_ERR_CLOSED : trevrpc_wt_unsupported();
}

intptr_t trevrpc_wt_stream_read_frame(trevrpc_wt_stream* stream, uint8_t** body, size_t* len, size_t max_len) {
    (void)max_len;
    if (stream == NULL || body == NULL || len == NULL) {
        return -EINVAL;
    }
    *body = NULL;
    *len = 0;
    return stream->closed ? TREV_WT_ERR_CLOSED : trevrpc_wt_unsupported();
}

intptr_t trevrpc_wt_stream_write(trevrpc_wt_stream* stream, const uint8_t* data, size_t len) {
    (void)data;
    (void)len;
    if (stream == NULL) {
        return -EINVAL;
    }
    return stream->closed ? TREV_WT_ERR_CLOSED : trevrpc_wt_unsupported();
}

int trevrpc_wt_stream_shutdown_send(trevrpc_wt_stream* stream) {
    if (stream == NULL) {
        return -EINVAL;
    }
    return stream->closed ? TREV_WT_ERR_CLOSED : trevrpc_wt_unsupported();
}

int trevrpc_wt_stream_abort(trevrpc_wt_stream* stream, uint32_t error_code) {
    (void)error_code;
    if (stream == NULL) {
        return -EINVAL;
    }
    stream->closed = 1;
    return 0;
}

void trevrpc_wt_stream_close(trevrpc_wt_stream* stream) {
    free(stream);
}

void trevrpc_wt_free(void* ptr) {
    free(ptr);
}

const char* trevrpc_wt_error(int code) {
    switch (code) {
    case 0:
        return "ok";
    case TREV_WT_ERR_CLOSED:
        return "closed";
    case TREV_WT_ERR_FRAME_TOO_LARGE:
        return "frame too large";
    case TREV_WT_ERR_REJECTED:
        return "WebTransport support is not implemented";
    case -ENOMEM:
    case ENOMEM:
        return "out of memory";
    case -EINVAL:
    case EINVAL:
        return "invalid argument";
    default:
        return "WebTransport operation failed";
    }
}
