#include "greeter.trevrpc.h"

#include <stddef.h>

static int say_hello(void* user_data,
    const trevrpc_call_context* context,
    const Installed__V1__HelloRequest* request,
    installed_v1_greeter_say_hello_respond_fn respond,
    void* respond_context) {
    (void)user_data;
    (void)context;
    (void)request;
    (void)respond;
    (void)respond_context;
    return TREVRPC_ERR_HANDLER_FAILED;
}

static int lots_of_replies(void* user_data,
    const trevrpc_call_context* context,
    const Installed__V1__HelloRequest* request,
    trevrpc_stream* stream) {
    (void)user_data;
    (void)context;
    (void)request;
    (void)stream;
    return TREVRPC_ERR_HANDLER_FAILED;
}

static int lots_of_greetings(void* user_data,
    const trevrpc_call_context* context,
    trevrpc_stream* stream,
    installed_v1_greeter_lots_of_greetings_respond_fn respond,
    void* respond_context) {
    (void)user_data;
    (void)context;
    (void)stream;
    (void)respond;
    (void)respond_context;
    return TREVRPC_ERR_HANDLER_FAILED;
}

static int bidi_hello(void* user_data, const trevrpc_call_context* context, trevrpc_stream* stream) {
    (void)user_data;
    (void)context;
    (void)stream;
    return TREVRPC_ERR_HANDLER_FAILED;
}

int main(void) {
    trevrpc_client_config_v1 client_config;
    trevrpc_server_config_v1 server_config;
    trevrpc_server_options_v1 server_options;
    trevrpc_call_options_v1 call_options;
    trevrpc_response_view_v1 response_view;
    trevrpc_status_view_v1 status_view;
    installed_v1_greeter_say_hello_result result = INSTALLED_V1_GREETER_SAY_HELLO_RESULT_INIT;
    installed_v1_greeter_hello_request_request_event request_event =
        INSTALLED_V1_GREETER_HELLO_REQUEST_REQUEST_EVENT_INIT;
    installed_v1_greeter_hello_request_request_receiver request_receiver =
        INSTALLED_V1_GREETER_HELLO_REQUEST_REQUEST_RECEIVER_INIT;
    installed_v1_greeter_hello_reply_event response_event = INSTALLED_V1_GREETER_HELLO_REPLY_EVENT_INIT;
    installed_v1_greeter_hello_reply_receiver response_receiver = INSTALLED_V1_GREETER_HELLO_REPLY_RECEIVER_INIT;
    installed_v1_greeter_server implementation = {
        .say_hello = say_hello,
        .lots_of_replies = lots_of_replies,
        .lots_of_greetings = lots_of_greetings,
        .bidi_hello = bidi_hello,
    };

    if (TREVRPC_C_ABI_VERSION != 6u || trevrpc_c_abi_version() != 6u ||
        trevrpc_client_config_v1_init(&client_config, sizeof(client_config)) != 0 ||
        trevrpc_server_config_v1_init(&server_config, sizeof(server_config)) != 0 ||
        trevrpc_server_options_v1_init(&server_options, sizeof(server_options)) != 0 ||
        trevrpc_call_options_v1_init(&call_options, sizeof(call_options)) != 0 ||
        trevrpc_response_view_v1_init(&response_view, sizeof(response_view)) != 0 ||
        trevrpc_status_view_v1_init(&status_view, sizeof(status_view)) != 0 || implementation.say_hello == NULL ||
        implementation.lots_of_replies == NULL || implementation.lots_of_greetings == NULL ||
        implementation.bidi_hello == NULL) {
        return 1;
    }

    installed_v1_greeter_say_hello_result_reset(&result);
    installed_v1_greeter_say_hello_result_reset(&result);
    installed_v1_greeter_hello_request_request_event_reset(&request_event);
    installed_v1_greeter_hello_request_request_event_reset(&request_event);
    installed_v1_greeter_hello_request_request_receiver_reset(&request_receiver);
    installed_v1_greeter_hello_reply_event_reset(&response_event);
    installed_v1_greeter_hello_reply_event_reset(&response_event);
    installed_v1_greeter_hello_reply_receiver_reset(&response_receiver);
    trevrpc_c_abi_6_anchor();
    return 0;
}
