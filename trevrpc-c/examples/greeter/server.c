#define _POSIX_C_SOURCE 200809L

#include "greeter.pb-c.h"
#include "greeter.trevrpc.h"
#include "transport.h"
#include "trevrpc_rpc_msquic.h"

#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SERVER_MAX_CALLS 64u
#define SERVER_WAIT_ATTEMPTS 60

static volatile sig_atomic_t stop_requested;

typedef struct server_reply {
    struct server_reply* next;
    char message[256];
} server_reply;

typedef struct server_call {
    bool used;
    bool accepted;
    bool request_fin;
    bool response_started;
    bool close_requested;
    size_t replies_sent;
    bool stream_closed;
    bool call_closed;
    uint32_t kind;
    trevrpc_rpc_call_v1 call;
    trevrpc_rpc_stream_v1 stream;
    char* name;
    size_t greeting_count;
    server_reply* reply_head;
    server_reply* reply_tail;
    bool send_pending;
} server_call;

typedef struct server_state {
    trevrpc_rpc_runtime* runtime;
    trevrpc_rpc_wake_source_v1 wake;
    trevrpc_rpc_endpoint_v1 listener;
    bool listener_ready;
    bool listener_closed;
    bool runtime_stopped;
    bool stopping;
    uint64_t next_operation;
    server_call calls[SERVER_MAX_CALLS];
    int error;
} server_state;

static void signal_handler(int signal_number) {
    (void)signal_number;
    stop_requested = 1;
}

static bool same_call(trevrpc_rpc_call_v1 left, trevrpc_rpc_call_v1 right) {
    return left.owner == right.owner && left.slot == right.slot && left.generation == right.generation;
}

static bool same_stream(trevrpc_rpc_stream_v1 left, trevrpc_rpc_stream_v1 right) {
    return left.owner == right.owner && left.slot == right.slot && left.generation == right.generation;
}

static void set_error(server_state* state, int error) {
    if (state->error == 0) {
        state->error = error != 0 ? error : -EIO;
    }
}

static int next_operation(server_state* state, uint64_t* operation) {
    if (state->next_operation == TREVRPC_RPC_OPERATION_ID_NONE || state->next_operation == UINT64_MAX) {
        return -EOVERFLOW;
    }
    *operation = state->next_operation++;
    return 0;
}

static server_call* find_call(server_state* state, trevrpc_rpc_call_v1 call) {
    for (size_t index = 0; index < SERVER_MAX_CALLS; ++index) {
        if (state->calls[index].used && same_call(state->calls[index].call, call)) {
            return &state->calls[index];
        }
    }
    return NULL;
}

static server_call* find_stream(server_state* state, trevrpc_rpc_stream_v1 stream) {
    for (size_t index = 0; index < SERVER_MAX_CALLS; ++index) {
        if (state->calls[index].used && same_stream(state->calls[index].stream, stream)) {
            return &state->calls[index];
        }
    }
    return NULL;
}

static server_call* allocate_call(server_state* state) {
    for (size_t index = 0; index < SERVER_MAX_CALLS; ++index) {
        if (!state->calls[index].used) {
            memset(&state->calls[index], 0, sizeof(state->calls[index]));
            state->calls[index].used = true;
            return &state->calls[index];
        }
    }
    return NULL;
}

static void release_call(server_state* state, server_call* call) {
    server_reply* reply;
    if (call == NULL || !call->used || !call->stream_closed || !call->call_closed) {
        return;
    }
    (void)trevrpc_rpc_stream_release(state->runtime, call->stream);
    (void)trevrpc_rpc_call_release(state->runtime, call->call);
    free(call->name);
    while ((reply = call->reply_head) != NULL) {
        call->reply_head = reply->next;
        free(reply);
    }
    memset(call, 0, sizeof(*call));
}

static void abort_call(server_state* state, server_call* call) {
    if (call == NULL || !call->used || call->close_requested) {
        return;
    }
    uint64_t operation;
    int rc = next_operation(state, &operation);
    if (rc == 0) {
        rc = trevrpc_rpc_call_close(
            state->runtime, call->call, operation, TREVRPC_RPC_CLOSE_FLAG_ABORT, TREVRPC_RPC_STATUS_INTERNAL);
    }
    if (rc != 0 && rc != -EALREADY && rc != -ESTALE) {
        set_error(state, rc);
    }
    uint64_t stream_operation;
    if (next_operation(state, &stream_operation) == 0) {
        int stream_rc = trevrpc_rpc_stream_close(
            state->runtime, call->stream, stream_operation, TREVRPC_RPC_CLOSE_FLAG_ABORT, TREVRPC_RPC_STATUS_INTERNAL);
        if (stream_rc != 0 && stream_rc != -EALREADY && stream_rc != -ESTALE) {
            set_error(state, stream_rc);
        }
    }
    call->close_requested = true;
}

static int decode_initial_name(const trevrpc_rpc_receive* receive,
    int (*decode)(const trevrpc_rpc_receive*, Hello__V1__HelloRequest**),
    char** out_name) {
    Hello__V1__HelloRequest* request = NULL;
    int rc = decode(receive, &request);
    if (rc != 0) {
        return rc;
    }
    const char* name = request->name != NULL ? request->name : "world";
    *out_name = strdup(name);
    hello__v1__hello_request__free_unpacked(request, NULL);
    return *out_name != NULL ? 0 : -ENOMEM;
}

static int send_reply_message(server_state* state, server_call* call, const char* message) {
    Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
    uint64_t operation;
    int rc = next_operation(state, &operation);
    if (rc != 0) {
        return rc;
    }
    reply.message = (char*)message;
    switch (call->kind) {
    case TREVRPC_RPC_KIND_SERVER_STREAMING:
        return hello_v1_greeter_lots_of_replies_send(state->runtime, call->stream, &reply, operation);
    case TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING:
        return hello_v1_greeter_bidi_hello_send_response(state->runtime, call->stream, &reply, operation);
    default:
        return -EINVAL;
    }
}

static int send_reply(server_state* state, server_call* call, const char* prefix, const char* name) {
    char message[256];
    if (snprintf(message, sizeof(message), "%s%s", prefix, name) >= (int)sizeof(message)) {
        return -EOVERFLOW;
    }
    return send_reply_message(state, call, message);
}

static int respond_reply(server_state* state, server_call* call, const char* message) {
    Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
    trevrpc_rpc_status_v1 status;
    uint64_t operation;
    int rc = trevrpc_rpc_status_v1_init(&status, sizeof(status));
    if (rc == 0) {
        rc = next_operation(state, &operation);
    }
    if (rc != 0) {
        return rc;
    }
    reply.message = (char*)message;
    if (call->kind == TREVRPC_RPC_KIND_UNARY) {
        return hello_v1_greeter_say_hello_respond(state->runtime, call->call, &reply, &status, operation);
    }
    if (call->kind == TREVRPC_RPC_KIND_CLIENT_STREAMING) {
        return hello_v1_greeter_lots_of_greetings_respond(state->runtime, call->call, &reply, &status, operation);
    }
    return -EINVAL;
}

static int finish_call(server_state* state, server_call* call) {
    trevrpc_rpc_status_v1 status;
    uint64_t operation;
    int rc = trevrpc_rpc_status_v1_init(&status, sizeof(status));
    if (rc == 0) {
        rc = next_operation(state, &operation);
    }
    if (rc != 0) {
        return rc;
    }
    if (call->kind == TREVRPC_RPC_KIND_SERVER_STREAMING) {
        return hello_v1_greeter_lots_of_replies_finish(state->runtime, call->call, operation, &status);
    }
    if (call->kind == TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        return hello_v1_greeter_bidi_hello_finish(state->runtime, call->call, operation, &status);
    }
    return -EINVAL;
}

static int queue_bidi_reply(server_call* call, const char* name) {
    server_reply* reply = malloc(sizeof(*reply));
    if (reply == NULL) {
        return -ENOMEM;
    }
    if (snprintf(reply->message, sizeof(reply->message), "Hello from bidi, %s", name) >= (int)sizeof(reply->message)) {
        free(reply);
        return -EOVERFLOW;
    }
    reply->next = NULL;
    if (call->reply_tail != NULL) {
        call->reply_tail->next = reply;
    } else {
        call->reply_head = reply;
    }
    call->reply_tail = reply;
    return 0;
}

static int pump_bidi_replies(server_state* state, server_call* call) {
    server_reply* reply;
    int rc;
    if (call->kind != TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING || call->send_pending) {
        return 0;
    }
    reply = call->reply_head;
    if (reply != NULL) {
        rc = send_reply_message(state, call, reply->message);
        if (rc == 0) {
            call->reply_head = reply->next;
            if (call->reply_head == NULL) {
                call->reply_tail = NULL;
            }
            call->send_pending = true;
            free(reply);
        }
        return rc;
    }
    if (!call->request_fin || call->response_started) {
        return 0;
    }
    rc = finish_call(state, call);
    if (rc == 0) {
        call->response_started = true;
    }
    return rc;
}

static int accept_call(server_state* state, server_call* call) {
    uint64_t operation;
    int rc = next_operation(state, &operation);
    if (rc != 0) {
        return rc;
    }
    switch (call->kind) {
    case TREVRPC_RPC_KIND_UNARY:
        rc = hello_v1_greeter_say_hello_accept(state->runtime, call->call, operation);
        break;
    case TREVRPC_RPC_KIND_SERVER_STREAMING:
        rc = hello_v1_greeter_lots_of_replies_accept(state->runtime, call->call, operation);
        break;
    case TREVRPC_RPC_KIND_CLIENT_STREAMING:
        rc = hello_v1_greeter_lots_of_greetings_accept(state->runtime, call->call, operation);
        break;
    case TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING:
        rc = hello_v1_greeter_bidi_hello_accept(state->runtime, call->call, operation);
        break;
    default:
        return -EPROTO;
    }
    if (rc == 0) {
        call->accepted = true;
    }
    return rc;
}

static int dispatch_finished_request(server_state* state, server_call* call) {
    char message[64];
    int rc;
    if (!call->accepted || !call->request_fin) {
        return 0;
    }
    if (call->kind == TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
        return pump_bidi_replies(state, call);
    }
    if (call->response_started) {
        return 0;
    }
    call->response_started = true;
    switch (call->kind) {
    case TREVRPC_RPC_KIND_UNARY: {
        char greeting[256];
        if (snprintf(greeting, sizeof(greeting), "Hello, %s", call->name != NULL ? call->name : "world") >=
            (int)sizeof(greeting)) {
            rc = -EOVERFLOW;
        } else {
            rc = respond_reply(state, call, greeting);
        }
        break;
    }
    case TREVRPC_RPC_KIND_SERVER_STREAMING:
        rc = send_reply(state, call, "Hello again, ", call->name != NULL ? call->name : "world");
        if (rc == 0)
            call->replies_sent = 1;
        break;
    case TREVRPC_RPC_KIND_CLIENT_STREAMING:
        snprintf(message, sizeof(message), "Received greetings: %zu", call->greeting_count);
        rc = respond_reply(state, call, message);
        break;
    default:
        rc = -EPROTO;
        break;
    }
    if (rc != 0) {
        abort_call(state, call);
    }
    return rc;
}

static int continue_server_stream(server_state* state, server_call* call) {
    int rc;
    if (call->kind != TREVRPC_RPC_KIND_SERVER_STREAMING || !call->response_started)
        return 0;
    if (call->replies_sent == 3)
        return finish_call(state, call);
    rc = send_reply(state, call, "Hello again, ", call->name != NULL ? call->name : "world");
    if (rc == 0)
        ++call->replies_sent;
    return rc;
}

static int handle_incoming(server_state* state, trevrpc_rpc_event* event, const trevrpc_rpc_event_info_v1* info) {
    server_call* call = allocate_call(state);
    trevrpc_rpc_receive* initial = NULL;
    int rc;
    if (call == NULL) {
        trevrpc_rpc_event_release(event);
        return -ENOSPC;
    }
    call->kind = info->rpc_kind;
    if (hello_v1_greeter_say_hello_matches_incoming(info)) {
        rc = hello_v1_greeter_say_hello_take_incoming(event, &call->call, &call->stream, &initial);
    } else if (hello_v1_greeter_lots_of_replies_matches_incoming(info)) {
        rc = hello_v1_greeter_lots_of_replies_take_incoming(event, &call->call, &call->stream, &initial);
    } else if (hello_v1_greeter_lots_of_greetings_matches_incoming(info)) {
        rc = hello_v1_greeter_lots_of_greetings_take_incoming(event, &call->call, &call->stream, &initial);
    } else if (hello_v1_greeter_bidi_hello_matches_incoming(info)) {
        rc = hello_v1_greeter_bidi_hello_take_incoming(event, &call->call, &call->stream, &initial);
    } else {
        rc = -EPROTO;
    }
    if (rc == 0 && initial == NULL) {
        rc = -EPROTO;
    }
    if (rc == 0 && (call->kind == TREVRPC_RPC_KIND_UNARY || call->kind == TREVRPC_RPC_KIND_SERVER_STREAMING)) {
        if (call->kind == TREVRPC_RPC_KIND_UNARY) {
            rc = decode_initial_name(initial, hello_v1_greeter_say_hello_decode_request_receive, &call->name);
        } else {
            rc = decode_initial_name(initial, hello_v1_greeter_lots_of_replies_decode_request_receive, &call->name);
        }
    }
    trevrpc_rpc_receive_release(initial);
    trevrpc_rpc_event_release(event);
    if (rc != 0) {
        if (call->call.owner != 0) {
            abort_call(state, call);
        } else {
            free(call->name);
            memset(call, 0, sizeof(*call));
        }
        return rc;
    }
    rc = accept_call(state, call);
    if (rc != 0) {
        abort_call(state, call);
    }
    return rc;
}

static int handle_receive(server_state* state, server_call* call) {
    for (;;) {
        trevrpc_rpc_receive* receive = NULL;
        trevrpc_rpc_receive_info_v1 info;
        int rc = trevrpc_rpc_stream_receive(state->runtime, call->stream, &receive);
        if (rc == -EAGAIN) {
            return call->kind == TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING ? pump_bidi_replies(state, call) : 0;
        }
        if (rc != 0) {
            return rc;
        }
        rc = trevrpc_rpc_receive_info_v1_init(&info, sizeof(info));
        if (rc == 0) {
            rc = trevrpc_rpc_receive_get_info_v1(receive, &info);
        }
        if (rc == 0 && info.kind == TREVRPC_RPC_RECEIVE_MESSAGE) {
            Hello__V1__HelloRequest* request = NULL;
            if (call->kind == TREVRPC_RPC_KIND_CLIENT_STREAMING) {
                rc = hello_v1_greeter_lots_of_greetings_decode_request_receive(receive, &request);
            } else if (call->kind == TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
                rc = hello_v1_greeter_bidi_hello_decode_request_receive(receive, &request);
            } else {
                rc = -EPROTO;
            }
            if (rc == 0) {
                const char* name = request->name != NULL ? request->name : "world";
                if (call->kind == TREVRPC_RPC_KIND_CLIENT_STREAMING) {
                    ++call->greeting_count;
                } else {
                    rc = queue_bidi_reply(call, name);
                }
            }
            hello__v1__hello_request__free_unpacked(request, NULL);
        } else if (rc == 0) {
            rc = -EPROTO;
        }
        trevrpc_rpc_receive_release(receive);
        if (rc != 0) {
            return rc;
        }
    }
}

static int drain_events(server_state* state) {
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
        rc = trevrpc_rpc_event_info_v1_init(&info, sizeof(info));
        if (rc == 0) {
            rc = trevrpc_rpc_event_get_info_v1(event, &info);
        }
        if (rc != 0) {
            trevrpc_rpc_event_release(event);
            set_error(state, rc);
            return state->error;
        }
        if (info.kind == TREVRPC_RPC_EVENT_ENDPOINT_READY && info.operation_id != 0) {
            state->listener_ready = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_ENDPOINT_CLOSED && info.endpoint.owner == state->listener.owner &&
                   info.endpoint.slot == state->listener.slot &&
                   info.endpoint.generation == state->listener.generation) {
            state->listener_closed = true;
        } else if (info.kind == TREVRPC_RPC_EVENT_CALL_INCOMING) {
            if (state->stopping) {
                trevrpc_rpc_event_release(event);
                event = NULL;
            } else {
                (void)handle_incoming(state, event, &info);
                event = NULL;
            }
        } else if (info.kind == TREVRPC_RPC_EVENT_CALL_ACCEPTED) {
            /* The accept completion is useful to consumers, but the call was marked admitted synchronously. */
        } else if (info.kind == TREVRPC_RPC_EVENT_SEND_COMPLETE) {
            server_call* call = find_stream(state, info.stream);
            if (call != NULL) {
                if (info.status != 0) {
                    rc = info.status;
                } else if (call->kind == TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING) {
                    call->send_pending = false;
                    rc = pump_bidi_replies(state, call);
                } else {
                    rc = continue_server_stream(state, call);
                }
                if (rc != 0) {
                    abort_call(state, call);
                }
            }
        } else if (info.kind == TREVRPC_RPC_EVENT_STREAM_READABLE) {
            server_call* call = find_stream(state, info.stream);
            if (call != NULL) {
                rc = handle_receive(state, call);
                if (rc != 0) {
                    abort_call(state, call);
                }
            }
        } else if (info.kind == TREVRPC_RPC_EVENT_STREAM_RECEIVE_FIN) {
            server_call* call = find_stream(state, info.stream);
            if (call != NULL) {
                call->request_fin = true;
                rc = dispatch_finished_request(state, call);
                if (rc != 0) {
                    abort_call(state, call);
                }
            }
        } else if (info.kind == TREVRPC_RPC_EVENT_STREAM_CLOSED) {
            server_call* call = find_stream(state, info.stream);
            if (call != NULL) {
                call->stream_closed = true;
                release_call(state, call);
            }
        } else if (info.kind == TREVRPC_RPC_EVENT_CALL_CLOSED) {
            server_call* call = find_call(state, info.call);
            if (call != NULL) {
                call->call_closed = true;
                release_call(state, call);
            }
        } else if ((info.flags & TREVRPC_RPC_EVENT_FLAG_FATAL) != 0 || info.kind == TREVRPC_RPC_EVENT_CALL_FAILED ||
                   info.kind == TREVRPC_RPC_EVENT_ENDPOINT_FAILED) {
            set_error(state, info.status != 0 ? info.status : -ECONNABORTED);
        } else if (info.kind == TREVRPC_RPC_EVENT_STOPPED) {
            state->runtime_stopped = true;
        }
        if (event != NULL) {
            trevrpc_rpc_event_release(event);
        }
        if (state->error != 0) {
            return state->error;
        }
    }
}

static int poll_runtime(server_state* state) {
    struct pollfd descriptor = {.fd = (int)state->wake.native_handle, .events = POLLIN, .revents = 0};
    int rc;
    do {
        rc = poll(&descriptor, 1, 1000);
    } while (rc < 0 && errno == EINTR);
    return rc < 0 ? -errno : 0;
}

static bool calls_empty(const server_state* state) {
    for (size_t index = 0; index < SERVER_MAX_CALLS; ++index) {
        if (state->calls[index].used) {
            return false;
        }
    }
    return true;
}

static void close_calls(server_state* state) {
    for (size_t index = 0; index < SERVER_MAX_CALLS; ++index) {
        if (state->calls[index].used) {
            abort_call(state, &state->calls[index]);
        }
    }
}

static int shutdown_server(server_state* state) {
    int rc = 0;
    state->stopping = true;
    close_calls(state);
    for (int attempt = 0; attempt < SERVER_WAIT_ATTEMPTS && !calls_empty(state); ++attempt) {
        int drain_rc = drain_events(state);
        if (drain_rc != 0 && rc == 0) {
            rc = drain_rc;
        }
        if (!calls_empty(state)) {
            int poll_rc = poll_runtime(state);
            if (poll_rc != 0 && rc == 0) {
                rc = poll_rc;
            }
        }
    }
    if (state->listener.owner != 0 && !state->listener_closed) {
        uint64_t operation;
        int close_rc = next_operation(state, &operation);
        if (close_rc == 0) {
            close_rc = trevrpc_rpc_endpoint_close(state->runtime, state->listener, operation);
            if (close_rc == -EALREADY || close_rc == -ESTALE) {
                close_rc = 0;
            }
        }
        for (int attempt = 0; close_rc == 0 && attempt < SERVER_WAIT_ATTEMPTS && !state->listener_closed; ++attempt) {
            int drain_rc = drain_events(state);
            if (drain_rc != 0 && rc == 0) {
                rc = drain_rc;
            }
            if (!state->listener_closed) {
                int poll_rc = poll_runtime(state);
                if (poll_rc != 0 && rc == 0) {
                    rc = poll_rc;
                }
            }
        }
        if (rc == 0 && close_rc != 0) {
            rc = close_rc;
        }
        if (state->listener_closed) {
            int release_rc = trevrpc_rpc_endpoint_release(state->runtime, state->listener);
            if (rc == 0 && release_rc != 0) {
                rc = release_rc;
            }
        }
    }
    if (state->runtime != NULL) {
        uint64_t operation;
        int close_rc = next_operation(state, &operation);
        if (close_rc == 0) {
            close_rc = trevrpc_rpc_runtime_close(state->runtime, operation);
            if (close_rc == -EALREADY) {
                close_rc = 0;
            }
        }
        for (int attempt = 0; close_rc == 0 && attempt < SERVER_WAIT_ATTEMPTS && !state->runtime_stopped; ++attempt) {
            int drain_rc = drain_events(state);
            if (drain_rc != 0 && rc == 0) {
                rc = drain_rc;
            }
            if (!state->runtime_stopped) {
                int poll_rc = poll_runtime(state);
                if (poll_rc != 0 && rc == 0) {
                    rc = poll_rc;
                }
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
    }
    return rc;
}

static int setup_server(
    server_state* state, const char* host, uint16_t port, const char* cert, const char* key, uint32_t transport) {
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
        endpoint_config.mode = TREVRPC_RPC_MSQUIC_ENDPOINT_LISTENER;
        endpoint_config.transport = transport;
        endpoint_config.host = host;
        endpoint_config.host_len = (uint32_t)strlen(host);
        endpoint_config.port = port;
        endpoint_config.cert_file = cert;
        endpoint_config.cert_file_len = (uint32_t)strlen(cert);
        endpoint_config.key_file = key;
        endpoint_config.key_file_len = (uint32_t)strlen(key);
        uint64_t operation;
        rc = next_operation(state, &operation);
        if (rc == 0) {
            rc = trevrpc_rpc_msquic_endpoint_start_v1(state->runtime, &endpoint_config, operation, &state->listener);
        }
    }
    if (rc == 0) {
        for (int attempt = 0; attempt < SERVER_WAIT_ATTEMPTS && !state->listener_ready; ++attempt) {
            rc = drain_events(state);
            if (rc == 0 && !state->listener_ready) {
                rc = poll_runtime(state);
            }
            if (rc != 0) {
                break;
            }
        }
        if (rc == 0 && !state->listener_ready) {
            rc = -ETIMEDOUT;
        }
    }
    return rc;
}

int main(int argc, char** argv) {
    const char* host = argc > 1 ? argv[1] : "127.0.0.1";
    uint16_t port = argc > 2 ? (uint16_t)strtoul(argv[2], NULL, 10) : 50051;
    const char* cert = argc > 3 ? argv[3] : "server.crt";
    const char* key = argc > 4 ? argv[4] : "server.key";
    const char* transport_name = argc > 5 ? argv[5] : "native";
    uint32_t transport = 0;
    server_state state = {.next_operation = 1};

    if (argc > 6 || trevrpc_example_transport_parse(transport_name, &transport) != 0) {
        fprintf(stderr, "usage: %s [host] [port] [cert] [key] [native|http3|webtransport]\n", argv[0]);
        return 1;
    }
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    int rc = setup_server(&state, host, port, cert, key, transport);
    if (rc == 0) {
        rc = trevrpc_rpc_endpoint_get_port_v1(state.runtime, state.listener, &port);
    }
    if (rc == 0) {
        printf("serving %s on %s:%u\n", transport_name, host, port);
        fflush(stdout);
        while (!stop_requested && state.error == 0) {
            rc = drain_events(&state);
            if (rc != 0) {
                break;
            }
            rc = poll_runtime(&state);
            if (rc != 0) {
                break;
            }
        }
    }
    if (rc != 0) {
        fprintf(stderr, "greeter server failed: %d\n", rc);
    }
    int shutdown_rc = shutdown_server(&state);
    if (rc == 0) {
        rc = shutdown_rc;
    }
    return rc == 0 ? 0 : 1;
}
