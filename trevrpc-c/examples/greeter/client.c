#define _POSIX_C_SOURCE 200809L

#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"
#include "transport.h"
#include "trevrpc_rpc_msquic.h"

#include <errno.h>
#include <poll.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define CLIENT_WAIT_TIMEOUT_NANOS UINT64_C(60000000000)

static bool same_stream(trevrpc_rpc_stream_v1 left, trevrpc_rpc_stream_v1 right) {
    return left.owner == right.owner && left.slot == right.slot && left.generation == right.generation;
}

static bool same_call(trevrpc_rpc_call_v1 left, trevrpc_rpc_call_v1 right) {
    return left.owner == right.owner && left.slot == right.slot && left.generation == right.generation;
}

typedef struct client_state {
    trevrpc_rpc_runtime* runtime;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 endpoint;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    uint32_t kind;
    uint64_t endpoint_operation;
    uint64_t call_operation;
    uint64_t send_operation;
    uint64_t finish_operation;
    uint64_t close_operation;
    bool endpoint_ready;
    bool endpoint_closed;
    bool call_ready;
    bool send_complete;
    bool send_finished;
    bool receive_fin;
    bool call_finished;
    bool stream_closed;
    bool call_closed;
    bool runtime_stopped;
    bool status_ok;
    unsigned replies;
    uint64_t events_seen;
    int error;
} client_state;

typedef int (*open_fn)(trevrpc_rpc_runtime*,
    trevrpc_rpc_endpoint_v1,
    const Hello__V1__HelloRequest*,
    uint64_t,
    trevrpc_rpc_call_v1*,
    trevrpc_rpc_stream_v1*);
typedef int (*send_fn)(trevrpc_rpc_runtime*, trevrpc_rpc_stream_v1, const Hello__V1__HelloRequest*, uint64_t);
typedef int (*finish_send_fn)(trevrpc_rpc_runtime*, trevrpc_rpc_stream_v1, uint64_t);
typedef int (*decode_reply_fn)(const trevrpc_rpc_receive*, Hello__V1__HelloReply**);

static decode_reply_fn decode_for_kind(uint32_t kind) {
    switch (kind) {
    case TREVRPC_RPC_KIND_UNARY:
        return hello_v1_greeter_say_hello_decode_response_receive;
    case TREVRPC_RPC_KIND_SERVER_STREAMING:
        return hello_v1_greeter_lots_of_replies_decode_response_receive;
    case TREVRPC_RPC_KIND_CLIENT_STREAMING:
        return hello_v1_greeter_lots_of_greetings_decode_response_receive;
    case TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING:
        return hello_v1_greeter_bidi_hello_decode_response_receive;
    default:
        return NULL;
    }
}

static void set_error(client_state* state, int error) {
    if (state->error == 0) {
        state->error = error != 0 ? error : -EIO;
    }
}

static int receive_response(
    client_state* state, trevrpc_rpc_receive* receive, decode_reply_fn decode_reply, const char* label) {
    trevrpc_rpc_receive_info_v1 info;
    int rc = trevrpc_rpc_receive_info_v1_init(&info, sizeof(info));
    if (rc == 0) {
        rc = trevrpc_rpc_receive_get_info_v1(receive, &info);
    }
    if (rc != 0) {
        return rc;
    }
    if (info.kind == TREVRPC_RPC_RECEIVE_MESSAGE) {
        Hello__V1__HelloReply* reply = NULL;
        if (decode_reply == NULL ||
            ((state->kind == TREVRPC_RPC_KIND_UNARY || state->kind == TREVRPC_RPC_KIND_CLIENT_STREAMING) &&
                state->replies != 0)) {
            return -EPROTO;
        }
        if (state->kind == TREVRPC_RPC_KIND_UNARY) {
            if (info.rpc_status != TREVRPC_RPC_STATUS_OK) {
                return -ECONNABORTED;
            }
            state->status_ok = true;
        }
        rc = decode_reply(receive, &reply);
        if (rc == 0) {
            printf("%s%s\n", label, reply->message != NULL ? reply->message : "");
            ++state->replies;
        }
        hello__v1__hello_reply__free_unpacked(reply, NULL);
        return rc;
    }
    if (info.kind == TREVRPC_RPC_RECEIVE_STATUS) {
        if (info.rpc_status != TREVRPC_RPC_STATUS_OK) {
            return -ECONNABORTED;
        }
        state->status_ok = true;
        return 0;
    }
    return -EPROTO;
}

static int drain_events(client_state* state, decode_reply_fn decode_reply, const char* label) {
    for (;;) {
        trevrpc_rpc_event* event = NULL;
        trevrpc_rpc_event_info_v1 info;
        int rc = trevrpc_rpc_runtime_next_event(state->runtime, &event);
        if (rc == -EAGAIN) {
            return state->error;
        }
        if (rc != 0) {
            set_error(state, rc);
            return state->error;
        }
        ++state->events_seen;
        rc = trevrpc_rpc_event_info_v1_init(&info, sizeof(info));
        if (rc == 0) {
            rc = trevrpc_rpc_event_get_info_v1(event, &info);
        }
        if (rc != 0) {
            trevrpc_rpc_event_release(event);
            set_error(state, rc);
            return state->error;
        }
        if (info.kind == TREVRPC_RPC_EVENT_ENDPOINT_READY && info.operation_id == state->endpoint_operation) {
            state->endpoint_ready = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED && info.endpoint.owner == state->endpoint.owner &&
                   info.endpoint.slot == state->endpoint.slot &&
                   info.endpoint.generation == state->endpoint.generation) {
            state->endpoint_closed = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_CALL_READY && info.operation_id == state->call_operation &&
                   same_call(info.call, state->call)) {
            state->call_ready = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_SEND_COMPLETE && info.operation_id == state->send_operation &&
                   same_stream(info.stream, state->stream)) {
            if (info.status != 0) {
                set_error(state, info.status);
            } else {
                state->send_complete = true;
            }
        } else if (info.kind == TREVRPC_RPC_EVENT_SEND_FINISHED && info.operation_id == state->finish_operation &&
                   same_stream(info.stream, state->stream)) {
            if (info.status != 0) {
                set_error(state, info.status);
            } else {
                state->send_finished = true;
            }
        } else if (info.kind == TREVRPC_RPC_EVENT_STREAM_READABLE && same_stream(info.stream, state->stream)) {
            for (;;) {
                trevrpc_rpc_receive* receive = NULL;
                rc = trevrpc_rpc_stream_receive(state->runtime, state->stream, &receive);
                if (rc == -EAGAIN) {
                    break;
                }
                if (rc != 0) {
                    set_error(state, rc);
                    break;
                }
                rc = receive_response(state, receive, decode_reply, label);
                trevrpc_rpc_receive_release(receive);
                if (rc != 0) {
                    set_error(state, rc);
                    break;
                }
            }
        } else if (info.kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN && same_stream(info.stream, state->stream)) {
            state->receive_fin = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_CALL_FINISHED && same_call(info.call, state->call)) {
            state->call_finished = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED && same_stream(info.stream, state->stream)) {
            state->stream_closed = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED && same_call(info.call, state->call)) {
            state->call_closed = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_STOPPED) {
            state->runtime_stopped = true;
        } else if ((info.flags & TREVRPC_RPC_EVENT_FLAG_FATAL) != 0 || info.kind == TREVRPC_RPC_EVENT_CALL_FAILED ||
                   info.kind == TREVRPC_RPC_EVENT_ENDPOINT_FAILED) {
            set_error(state, info.status != 0 ? info.status : -ECONNABORTED);
        }
        trevrpc_rpc_event_release(event);
        if (state->error != 0) {
            return state->error;
        }
    }
}

typedef bool (*condition_fn)(const client_state* state);

static uint64_t monotonic_nanos(void) {
    struct timespec now;
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0) {
        return 0;
    }
    return (uint64_t)now.tv_sec * UINT64_C(1000000000) + (uint64_t)now.tv_nsec;
}

static int poll_until(
    client_state* state, decode_reply_fn decode_reply, const char* label, condition_fn condition, bool preserve_error) {
    uint64_t deadline = monotonic_nanos() + CLIENT_WAIT_TIMEOUT_NANOS;
    int saved_error = state->error;
    if (!preserve_error) {
        state->error = 0;
    }
    for (;;) {
        uint64_t events_before = state->events_seen;
        int rc = drain_events(state, decode_reply, label);
        if (preserve_error && rc != 0) {
            state->error = saved_error;
            return rc;
        }
        if (condition(state)) {
            if (!preserve_error) {
                state->error = saved_error;
            }
            return 0;
        }
        uint64_t now = monotonic_nanos();
        if (now == 0 || now >= deadline) {
            state->error = saved_error;
            return -ETIMEDOUT;
        }
        uint64_t remaining = deadline - now;
        int timeout = remaining > UINT64_C(1000000000) ? 1000 : (int)((remaining + 999999u) / 1000000u);
        struct pollfd descriptor = {.fd = (int)state->wake.native_handle, .events = POLLIN, .revents = 0};
        do {
            rc = poll(&descriptor, 1, timeout);
        } while (rc < 0 && errno == EINTR);
        if (rc < 0) {
            state->error = saved_error;
            return -errno;
        }
        if (rc > 0 && state->events_seen == events_before) {
            struct timespec delay = {.tv_sec = 0, .tv_nsec = 1000000};
            (void)nanosleep(&delay, NULL);
        }
    }
}

static int wait_for(client_state* state, decode_reply_fn decode_reply, const char* label, condition_fn condition) {
    return poll_until(state, decode_reply, label, condition, true);
}

static bool endpoint_ready(const client_state* state) {
    return state->endpoint_ready;
}

static bool call_ready(const client_state* state) {
    return state->call_ready;
}

static bool send_complete(const client_state* state) {
    return state->send_complete;
}

static bool send_finished(const client_state* state) {
    return state->send_finished;
}

static bool unary_done(const client_state* state) {
    return state->replies == 1 && state->receive_fin && state->status_ok;
}

static bool streaming_done(const client_state* state) {
    return state->replies >= 3 && state->receive_fin && state->status_ok;
}

static bool client_stream_done(const client_state* state) {
    return state->replies == 1 && state->receive_fin && state->status_ok;
}

static bool bidi_done(const client_state* state) {
    return state->replies >= 2 && state->receive_fin && state->status_ok;
}

static bool closed(const client_state* state) {
    return state->stream_closed && state->call_closed;
}

static bool endpoint_closed(const client_state* state) {
    return state->endpoint_closed;
}

static bool stopped(const client_state* state) {
    return state->runtime_stopped;
}

static int wait_for_cleanup(
    client_state* state, decode_reply_fn decode_reply, const char* label, condition_fn condition) {
    return poll_until(state, decode_reply, label, condition, false);
}

static int next_operation(client_state* state, uint64_t* operation) {
    static uint64_t next = 1;
    (void)state;
    if (next == TREVRPC_RPC_OPERATION_ID_NONE) {
        return -EOVERFLOW;
    }
    *operation = next++;
    return 0;
}

static int begin_call(client_state* state,
    uint32_t kind,
    open_fn open,
    const Hello__V1__HelloRequest* initial_or_null,
    decode_reply_fn decode_reply,
    const char* label) {
    int rc = next_operation(state, &state->call_operation);
    if (rc != 0) {
        return rc;
    }
    state->kind = kind;
    state->call = (trevrpc_rpc_call_v1){0};
    state->stream = (trevrpc_rpc_stream_v1){0};
    state->call_ready = false;
    state->send_complete = false;
    state->send_finished = false;
    state->receive_fin = false;
    state->call_finished = false;
    state->stream_closed = false;
    state->call_closed = false;
    state->status_ok = false;
    state->replies = 0;
    rc = open(state->runtime, state->endpoint, initial_or_null, state->call_operation, &state->call, &state->stream);
    if (rc != 0) {
        return rc;
    }
    return wait_for(state, decode_reply, label, call_ready);
}

static int send_request(
    client_state* state, send_fn send, const char* name, decode_reply_fn decode_reply, const char* label) {
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    uint64_t operation;
    int rc = next_operation(state, &operation);
    if (rc != 0) {
        return rc;
    }
    request.name = (char*)name;
    state->send_complete = false;
    state->send_operation = operation;
    rc = send(state->runtime, state->stream, &request, operation);
    if (rc != 0) {
        return rc;
    }
    return wait_for(state, decode_reply, label, send_complete);
}

static int finish_request(client_state* state, finish_send_fn finish, decode_reply_fn decode_reply, const char* label) {
    uint64_t operation;
    int rc = next_operation(state, &operation);
    if (rc != 0) {
        return rc;
    }
    state->send_finished = false;
    state->finish_operation = operation;
    rc = finish(state->runtime, state->stream, operation);
    if (rc != 0) {
        return rc;
    }
    return wait_for(state, decode_reply, label, send_finished);
}

static int close_call(client_state* state, decode_reply_fn decode_reply, const char* label) {
    uint64_t operation;
    int rc = next_operation(state, &operation);
    if (rc != 0) {
        return rc;
    }
    state->close_operation = operation;
    rc = trevrpc_rpc_call_close(state->runtime, state->call, operation, TREVRPC_RPC_CLOSE_FLAG_NONE, 0);
    if (rc != 0 && rc != -EALREADY && rc != -ESTALE) {
        return rc;
    }
    rc = state->error == 0 ? wait_for(state, decode_reply, label, closed)
                           : wait_for_cleanup(state, decode_reply, label, closed);
    if (rc != 0) {
        return rc;
    }
    int release_rc = trevrpc_rpc_stream_release(state->runtime, state->stream);
    if (release_rc == 0) {
        release_rc = trevrpc_rpc_call_release(state->runtime, state->call);
    }
    if (release_rc != 0) {
        return release_rc;
    }
    state->stream = (trevrpc_rpc_stream_v1){0};
    state->call = (trevrpc_rpc_call_v1){0};
    return 0;
}

static int setup_client(client_state* state, const char* host, uint16_t port, uint32_t transport) {
    trevrpc_rpc_runtime_config_v1 runtime_config;
    trevrpc_rpc_msquic_config_v1 provider_config;
    trevrpc_rpc_msquic_endpoint_config_v1 endpoint_config;
    int rc = trevrpc_rpc_runtime_config_v1_init(&runtime_config, sizeof(runtime_config));
    if (rc == 0) {
        rc = trevrpc_rpc_msquic_config_v1_init(&provider_config, sizeof(provider_config));
    }
    if (rc == 0) {
        rc = trevrpc_rpc_msquic_create_v1(&runtime_config, &provider_config, &state->runtime);
    }
    if (rc == 0) {
        rc = trevrpc_rpc_wake_source_v1_init(&state->wake, sizeof(state->wake));
    }
    if (rc == 0) {
        rc = trevrpc_rpc_runtime_get_wake_source_v1(state->runtime, &state->wake);
    }
    if (rc == 0) {
        rc = trevrpc_rpc_msquic_endpoint_config_v1_init(&endpoint_config, sizeof(endpoint_config));
    }
    if (rc == 0) {
        endpoint_config.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_CLIENT;
        endpoint_config.transport = transport;
        endpoint_config.host = host;
        endpoint_config.host_len = (uint32_t)strlen(host);
        endpoint_config.server_name = host;
        endpoint_config.server_name_len = (uint32_t)strlen(host);
        endpoint_config.port = port;
        endpoint_config.flags &= ~TREVRPC_RPC_MSQUIC_VERIFY_PEER;
        rc = next_operation(state, &state->endpoint_operation);
    }
    if (rc == 0) {
        rc = trevrpc_rpc_msquic_endpoint_start_v1(
            state->runtime, &endpoint_config, state->endpoint_operation, &state->endpoint);
    }
    if (rc == 0) {
        rc = wait_for(state, NULL, "", endpoint_ready);
    }
    return rc;
}

static int shutdown_client(client_state* state) {
    int rc = 0;
    if (state->runtime == NULL) {
        return 0;
    }
    if (state->call.owner != 0 && !closed(state)) {
        int close_rc = close_call(state, decode_for_kind(state->kind), "");
        if (rc == 0 && close_rc != 0) {
            rc = close_rc;
        }
    }
    if (state->endpoint.owner != 0 && !state->endpoint_closed) {
        uint64_t operation;
        int close_rc = next_operation(state, &operation);
        if (close_rc == 0) {
            close_rc = trevrpc_rpc_endpoint_close(state->runtime, state->endpoint, operation);
            if (close_rc == -EALREADY || close_rc == -ESTALE) {
                close_rc = 0;
            } else if (close_rc == 0) {
                close_rc = wait_for_cleanup(state, NULL, "", endpoint_closed);
            }
        }
        if (rc == 0 && close_rc != 0) {
            rc = close_rc;
        }
    }
    if (state->endpoint.owner != 0 && state->endpoint_closed) {
        int release_rc = trevrpc_rpc_endpoint_release(state->runtime, state->endpoint);
        if (rc == 0 && release_rc != 0) {
            rc = release_rc;
        }
        state->endpoint = (trevrpc_rpc_endpoint_v1){0};
    }
    int close_rc = next_operation(state, &state->close_operation);
    if (close_rc == 0) {
        close_rc = trevrpc_rpc_runtime_close(state->runtime, state->close_operation);
        if (close_rc == -EALREADY) {
            close_rc = 0;
        } else if (close_rc == 0) {
            close_rc = wait_for_cleanup(state, NULL, "", stopped);
        }
    }
    if (rc == 0 && close_rc != 0) {
        rc = close_rc;
    }
    int drain_rc = trevrpc_rpc_runtime_drain(state->runtime);
    if (rc == 0 && drain_rc != 0) {
        rc = drain_rc;
    }
    int release_rc = trevrpc_rpc_runtime_release(state->runtime);
    if (rc == 0 && release_rc != 0) {
        rc = release_rc;
    }
    state->runtime = NULL;
    return rc;
}

static int run_client(client_state* state, const char* name) {
    Hello__V1__HelloRequest request = HELLO__V1__HELLO_REQUEST__INIT;
    request.name = (char*)name;
    int rc = begin_call(state,
        TREVRPC_RPC_KIND_UNARY,
        hello_v1_greeter_say_hello_open,
        &request,
        hello_v1_greeter_say_hello_decode_response_receive,
        "unary: ");
    if (rc == 0) {
        rc = wait_for(state, hello_v1_greeter_say_hello_decode_response_receive, "unary: ", unary_done);
    }
    if (rc == 0) {
        rc = close_call(state, hello_v1_greeter_say_hello_decode_response_receive, "unary: ");
    }

    if (rc == 0) {
        printf("server streaming:\n");
        rc = begin_call(state,
            TREVRPC_RPC_KIND_SERVER_STREAMING,
            hello_v1_greeter_lots_of_replies_open,
            &request,
            hello_v1_greeter_lots_of_replies_decode_response_receive,
            "  ");
    }
    if (rc == 0) {
        rc = wait_for(state, hello_v1_greeter_lots_of_replies_decode_response_receive, "  ", streaming_done);
    }
    if (rc == 0) {
        rc = close_call(state, hello_v1_greeter_lots_of_replies_decode_response_receive, "  ");
    }

    if (rc == 0) {
        printf("client streaming:\n");
        rc = begin_call(state,
            TREVRPC_RPC_KIND_CLIENT_STREAMING,
            hello_v1_greeter_lots_of_greetings_open,
            NULL,
            hello_v1_greeter_lots_of_greetings_decode_response_receive,
            "  ");
    }
    if (rc == 0) {
        rc = send_request(state,
            hello_v1_greeter_lots_of_greetings_send,
            "Alice",
            hello_v1_greeter_lots_of_greetings_decode_response_receive,
            "  ");
    }
    if (rc == 0) {
        rc = send_request(state,
            hello_v1_greeter_lots_of_greetings_send,
            "Bob",
            hello_v1_greeter_lots_of_greetings_decode_response_receive,
            "  ");
    }
    if (rc == 0) {
        rc = finish_request(state,
            hello_v1_greeter_lots_of_greetings_finish_send,
            hello_v1_greeter_lots_of_greetings_decode_response_receive,
            "  ");
    }
    if (rc == 0) {
        rc = wait_for(state, hello_v1_greeter_lots_of_greetings_decode_response_receive, "  ", client_stream_done);
    }
    if (rc == 0) {
        rc = close_call(state, hello_v1_greeter_lots_of_greetings_decode_response_receive, "  ");
    }

    if (rc == 0) {
        printf("bidi streaming:\n");
        rc = begin_call(state,
            TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING,
            hello_v1_greeter_bidi_hello_open,
            NULL,
            hello_v1_greeter_bidi_hello_decode_response_receive,
            "  ");
    }
    if (rc == 0) {
        rc = send_request(state,
            hello_v1_greeter_bidi_hello_send_request,
            "Carol",
            hello_v1_greeter_bidi_hello_decode_response_receive,
            "  ");
    }
    if (rc == 0) {
        rc = send_request(state,
            hello_v1_greeter_bidi_hello_send_request,
            "Dave",
            hello_v1_greeter_bidi_hello_decode_response_receive,
            "  ");
    }
    if (rc == 0) {
        rc = finish_request(
            state, hello_v1_greeter_bidi_hello_finish_send, hello_v1_greeter_bidi_hello_decode_response_receive, "  ");
    }
    if (rc == 0) {
        rc = wait_for(state, hello_v1_greeter_bidi_hello_decode_response_receive, "  ", bidi_done);
    }
    if (rc == 0) {
        rc = close_call(state, hello_v1_greeter_bidi_hello_decode_response_receive, "  ");
    }
    return rc;
}

int main(int argc, char** argv) {
    const char* host = argc > 1 ? argv[1] : "127.0.0.1";
    uint16_t port = argc > 2 ? (uint16_t)strtoul(argv[2], NULL, 10) : 50051;
    const char* name = argc > 3 ? argv[3] : "TrevRPC";
    const char* transport_name = argc > 4 ? argv[4] : "native";
    uint32_t transport = 0;
    client_state state = {0};

    if (argc > 5 || trevrpc_example_transport_parse(transport_name, &transport) != 0) {
        fprintf(stderr, "usage: %s [host] [port] [name] [native|http3|webtransport]\n", argv[0]);
        return 1;
    }
    int rc = setup_client(&state, host, port, transport);
    if (rc == 0) {
        rc = run_client(&state, name);
    }
    if (rc != 0) {
        fprintf(stderr, "greeter client failed: %d\n", rc);
    }
    int shutdown_rc = shutdown_client(&state);
    if (rc == 0) {
        rc = shutdown_rc;
    }
    return rc == 0 ? 0 : 1;
}
