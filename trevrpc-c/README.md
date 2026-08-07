# trevrpc-c

TrevRPC is an RPC framework like gRPC, but uses QUIC (and HTTP/3 / WebTransport) instead of HTTP/2. Define services in protobuf, generate typed clients and servers, and run them over QUIC.

Full documentation: https://trev.zip/llc/TrevRPC/wiki

## Protobuf

```proto
syntax = "proto3";

package hello.v1;

service Greeter {
  rpc SayHello(HelloRequest) returns (HelloReply);
  rpc LotsOfReplies(HelloRequest) returns (stream HelloReply);
  rpc LotsOfGreetings(stream HelloRequest) returns (HelloReply);
  rpc BidiHello(stream HelloRequest) returns (stream HelloReply);
}

message HelloRequest { string name = 1; }
message HelloReply { string message = 1; }
```

Generate with `protoc-c` + `protoc-gen-trevrpc-c`.

## Client

```c
#include <trevrpc.h>

trevrpc_client_config_v1 cfg;
trevrpc_client_config_v1_init(&cfg, sizeof(cfg));

trevrpc_channel *ch = NULL;
trevrpc_channel_connect_v1("127.0.0.1", 50051, &cfg, NULL, 5000000000ull, NULL, &ch);

// Unary
Hello__V1__HelloRequest req = HELLO__V1__HELLO_REQUEST__INIT;
req.name = "TrevRPC";
hello_v1_greeter_say_hello_result res = HELLO_V1_GREETER_SAY_HELLO_RESULT_INIT;
hello_v1_greeter_say_hello(ch, &req, &res);
printf("%s\n", res.response->message);
hello_v1_greeter_say_hello_result_reset(&res);

// Server streaming
trevrpc_stream *stream = NULL;
hello_v1_greeter_lots_of_replies(ch, &req, &stream);
hello_v1_greeter_hello_reply_receiver recv = HELLO_V1_GREETER_HELLO_REPLY_RECEIVER_INIT;
hello_v1_greeter_hello_reply_receiver_init(&recv, stream);
hello_v1_greeter_hello_reply_event ev = HELLO_V1_GREETER_HELLO_REPLY_EVENT_INIT;
while (hello_v1_greeter_recv_hello_v1_hello_reply(&recv, &ev) == 0) {
    if (ev.kind == HELLO_V1_GREETER_HELLO_REPLY_EVENT_MESSAGE) printf("%s\n", ev.message->message);
    if (ev.kind != HELLO_V1_GREETER_HELLO_REPLY_EVENT_MESSAGE) break;
    hello_v1_greeter_hello_reply_event_reset(&ev);
}
trevrpc_stream_close(stream);

// Client streaming
trevrpc_stream *cs = NULL;
hello_v1_greeter_lots_of_greetings_start(ch, &cs);
hello_v1_greeter_send_hello_v1_hello_request(cs, &req);
hello_v1_greeter_send_hello_v1_hello_request(cs, &req);
trevrpc_stream_finish_send(cs);
// then receive single reply via receiver as above

// Bidi: same as client streaming but interleave send/recv and finish with trevrpc_stream_finish_send

trevrpc_channel_close(ch);
trevrpc_channel_release(ch);
```

## Server

```c
static int say_hello(void *ud, const trevrpc_call_context *ctx,
    const Hello__V1__HelloRequest *req,
    hello_v1_greeter_say_hello_respond_fn respond, void *rctx) {
    Hello__V1__HelloReply reply = HELLO__V1__HELLO_REPLY__INIT;
    reply.message = req->name;
    hello_v1_greeter_say_hello_response_view v = { .message = &reply, .status = TREVRPC_STATUS_OK };
    return respond(rctx, &v);
}
static int lots_of_replies(void *ud, const trevrpc_call_context *ctx,
    const Hello__V1__HelloRequest *req, trevrpc_server_stream *s) {
    Hello__V1__HelloReply r = HELLO__V1__HELLO_REPLY__INIT;
    r.message = req->name;
    hello_v1_greeter_send_hello_v1_hello_reply(s, &r);
    return 0; // OK status sent automatically on success
}
// lots_of_greetings / bidi_hello: receive with trevrpc_server_stream_recv, send with send_hello_v1_hello_reply

trevrpc_server *srv = NULL;
trevrpc_server_config_v1 scfg;
trevrpc_server_config_v1_init(&scfg, sizeof(scfg));
scfg.host = "127.0.0.1"; scfg.port = 50051;
trevrpc_server_listen_v1(&scfg, &srv);
hello_v1_greeter_server svc = { .say_hello = say_hello, .lots_of_replies = lots_of_replies /* ... */ };
hello_v1_greeter_register(srv, &svc);
trevrpc_server_freeze(srv);
trevrpc_server_serve(srv);
```
