//go:build trevrpc_msquic_native && cgo

package cruntimetest

/*
#cgo CFLAGS: -I${SRCDIR}/../../../trevrpc-c/include -I${SRCDIR}/../../../trevrpc-c/src
#cgo LDFLAGS: -lmsquic -lpthread
#define _POSIX_C_SOURCE 200809L
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include "trevrpc_msquic.c"
#include "trevrpc_values.c"
#include "trevrpc_wire.c"
#include "trevrpc.c"

static int trevrpc_c_echo_handler(void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_response* response) {
    (void)user_data;
    (void)context;
    return trevrpc_response_set_body(response, request->body, request->body_len);
}

static int trevrpc_c_deadline_handler(void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_response* response) {
    (void)user_data;
    if (!trevrpc_call_context_has_deadline(context)) {
        return -EINVAL;
    }
    while (!trevrpc_call_context_deadline_expired(context)) {}
    return trevrpc_response_set_body(response, request->body, request->body_len);
}

static int trevrpc_c_server_stream_handler(void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_stream* stream) {
    (void)user_data;
    (void)context;
    int err = trevrpc_stream_send_message(stream, request->body, request->body_len);
    if (err != 0) {
        return err;
    }
    return trevrpc_stream_send_message(stream, request->body, request->body_len);
}

static int trevrpc_c_register_echo(trevrpc_server* server) {
    int err = trevrpc_server_register_unary(server, "test.EchoService", "Echo", trevrpc_c_echo_handler, NULL);
    if (err != 0) {
        return err;
    }
    err = trevrpc_server_register_unary(server, "test.EchoService", "Deadline", trevrpc_c_deadline_handler, NULL);
    if (err != 0) {
        return err;
    }
    return trevrpc_server_register_streaming(server, "test.StreamService", "ServerStream", TREVRPC_RPC_KIND_SERVER_STREAMING, trevrpc_c_server_stream_handler, NULL);
}

static void* trevrpc_c_serve_thread(void* arg) {
    int code = trevrpc_server_serve((trevrpc_server*)arg);
    return (void*)(intptr_t)code;
}

static int trevrpc_c_server_start(trevrpc_server* server, pthread_t* thread) {
    int err = pthread_create(thread, NULL, trevrpc_c_serve_thread, server);
    return err == 0 ? 0 : -err;
}

static int trevrpc_c_server_join(pthread_t thread) {
    void* value = NULL;
    int err = pthread_join(thread, &value);
    if (err != 0) {
        return -err;
    }
    return (int)(intptr_t)value;
}

static int trevrpc_c_call_unary_with_timeout(trevrpc_client* client, const char* service, const char* method, const uint8_t* body, size_t body_len, uint64_t timeout_nanos, trevrpc_response** out_response) {
    if (client == NULL || out_response == NULL || (body == NULL && body_len > 0)) {
        return -EINVAL;
    }
    *out_response = NULL;

    trevrpc_msquic_stream* stream = NULL;
    int err = trevrpc_msquic_conn_open_stream(client->conn, &stream);
    if (err != 0) {
        return err;
    }

    uint8_t* frame = NULL;
    size_t frame_len = 0;
    err = trevrpc_wire_encode_request(service, method, TREVRPC_RPC_KIND_UNARY, body, body_len, NULL, timeout_nanos, client->max_frame_size, &frame, &frame_len);
    if (err == 0) {
        err = trevrpc_write_frame(stream, frame, frame_len);
    }
    free(frame);
    if (err == 0) {
        err = trevrpc_msquic_stream_shutdown_send(stream);
    }

    uint8_t* response_body = NULL;
    size_t response_body_len = 0;
    if (err == 0) {
        intptr_t read = trevrpc_msquic_stream_read_frame(stream, &response_body, &response_body_len, client->max_frame_size);
        if (read < 0) {
            err = (int)read;
        } else if (read == 0) {
            err = TREV_MSQUIC_ERR_CLOSED;
        }
    }
    if (err == 0) {
        err = trevrpc_wire_decode_response(response_body, response_body_len, out_response);
    }

    trevrpc_msquic_free(response_body);
    trevrpc_msquic_stream_close(stream);
    return err;
}
*/
import "C"

import (
	"fmt"
	"unsafe"
)

const statusOK = 0
const statusInvalidArgument = 3
const statusDeadlineExceeded = 4
const statusUnavailable = 14

type runtimeServer struct {
	ptr     *C.trevrpc_server
	thread  C.pthread_t
	started bool
}

func startEchoServer(host, certFile, keyFile string, port uint16) (*runtimeServer, error) {
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))
	cCertFile := C.CString(certFile)
	defer C.free(unsafe.Pointer(cCertFile))
	cKeyFile := C.CString(keyFile)
	defer C.free(unsafe.Pointer(cKeyFile))

	serverConfig := C.trevrpc_default_config()
	serverConfig.cert_file = cCertFile
	serverConfig.key_file = cKeyFile
	wtConfig := C.trevrpc_wt_config{}
	wtConfig.path = C.CString("/trevrpc")
	defer C.free(unsafe.Pointer(wtConfig.path))
	wtConfig.cert_file = cCertFile
	wtConfig.key_file = cKeyFile

	var server *C.trevrpc_server
	if code := C.trevrpc_server_listen(cHost, C.uint16_t(port), &wtConfig, &serverConfig, &server); code != 0 {
		return nil, runtimeError("listen", code)
	}

	if code := C.trevrpc_c_register_echo(server); code != 0 {
		C.trevrpc_server_close(server)
		return nil, runtimeError("register handlers", code)
	}

	runtimeServer := &runtimeServer{ptr: server}
	if code := C.trevrpc_c_server_start(server, &runtimeServer.thread); code != 0 {
		C.trevrpc_server_close(server)
		return nil, runtimeError("start serve thread", code)
	}
	runtimeServer.started = true
	return runtimeServer, nil
}

func (s *runtimeServer) close() error {
	if s == nil || s.ptr == nil {
		return nil
	}

	C.trevrpc_server_shutdown(s.ptr)
	var err error
	if s.started {
		if code := C.trevrpc_c_server_join(s.thread); code != 0 {
			err = runtimeError("join serve thread", code)
		}
		s.started = false
	}
	C.trevrpc_server_close(s.ptr)
	s.ptr = nil
	return err
}

func callUnary(host string, port uint16, service, method string, body []byte) ([]byte, uint32, error) {
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))
	cService := C.CString(service)
	defer C.free(unsafe.Pointer(cService))
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))

	clientConfig := C.trevrpc_default_config()
	var client *C.trevrpc_client
	if code := C.trevrpc_client_connect(cHost, C.uint16_t(port), &clientConfig, &client); code != 0 {
		return nil, 0, runtimeError("connect client", code)
	}
	defer C.trevrpc_client_close(client)

	var bodyPtr *C.uint8_t
	if len(body) > 0 {
		bodyPtr = (*C.uint8_t)(unsafe.Pointer(&body[0]))
	}

	var response *C.trevrpc_response
	if code := C.trevrpc_client_call_unary(
		client,
		cService,
		cMethod,
		bodyPtr,
		C.size_t(len(body)),
		&response,
	); code != 0 {
		return nil, 0, runtimeError("call unary", code)
	}
	defer C.trevrpc_response_free(response)

	var responseBody []byte
	if response.body_len > 0 {
		responseBody = C.GoBytes(unsafe.Pointer(response.body), C.int(response.body_len))
	}
	return responseBody, uint32(response.status), nil
}

func callUnaryWithTimeout(host string, port uint16, service, method string, body []byte, timeoutNanos uint64) ([]byte, uint32, error) {
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))
	cService := C.CString(service)
	defer C.free(unsafe.Pointer(cService))
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))

	clientConfig := C.trevrpc_default_config()
	var client *C.trevrpc_client
	if code := C.trevrpc_client_connect(cHost, C.uint16_t(port), &clientConfig, &client); code != 0 {
		return nil, 0, runtimeError("connect timeout client", code)
	}
	defer C.trevrpc_client_close(client)

	var bodyPtr *C.uint8_t
	if len(body) > 0 {
		bodyPtr = (*C.uint8_t)(unsafe.Pointer(&body[0]))
	}

	var response *C.trevrpc_response
	if code := C.trevrpc_c_call_unary_with_timeout(
		client,
		cService,
		cMethod,
		bodyPtr,
		C.size_t(len(body)),
		C.uint64_t(timeoutNanos),
		&response,
	); code != 0 {
		return nil, 0, runtimeError("call unary with timeout", code)
	}
	defer C.trevrpc_response_free(response)

	var responseBody []byte
	if response.body_len > 0 {
		responseBody = C.GoBytes(unsafe.Pointer(response.body), C.int(response.body_len))
	}
	return responseBody, uint32(response.status), nil
}

func callServerStream(host string, port uint16, body []byte) ([][]byte, uint32, error) {
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))
	cService := C.CString("test.StreamService")
	defer C.free(unsafe.Pointer(cService))
	cMethod := C.CString("ServerStream")
	defer C.free(unsafe.Pointer(cMethod))

	clientConfig := C.trevrpc_default_config()
	var client *C.trevrpc_client
	if code := C.trevrpc_client_connect(cHost, C.uint16_t(port), &clientConfig, &client); code != 0 {
		return nil, 0, runtimeError("connect streaming client", code)
	}
	defer C.trevrpc_client_close(client)

	var bodyPtr *C.uint8_t
	if len(body) > 0 {
		bodyPtr = (*C.uint8_t)(unsafe.Pointer(&body[0]))
	}

	var stream *C.trevrpc_stream
	if code := C.trevrpc_client_start_stream(
		client,
		cService,
		cMethod,
		C.TREVRPC_RPC_KIND_SERVER_STREAMING,
		bodyPtr,
		C.size_t(len(body)),
		&stream,
	); code != 0 {
		return nil, 0, runtimeError("start server stream", code)
	}
	defer C.trevrpc_stream_close(stream)
	if code := C.trevrpc_stream_finish_send(stream); code != 0 {
		return nil, 0, runtimeError("finish stream send", code)
	}

	var messages [][]byte
	for {
		var frame *C.trevrpc_stream_frame
		if code := C.trevrpc_stream_recv(stream, &frame); code != 0 {
			return nil, 0, runtimeError("recv stream frame", code)
		}
		if frame == nil {
			return messages, statusUnavailable, nil
		}
		if frame.kind == C.TREVRPC_STREAM_FRAME_KIND_STATUS {
			status := uint32(frame.status)
			C.trevrpc_stream_frame_free(frame)
			return messages, status, nil
		}
		messages = append(messages, C.GoBytes(unsafe.Pointer(frame.body), C.int(frame.body_len)))
		C.trevrpc_stream_frame_free(frame)
	}
}

func runtimeError(operation string, code C.int) error {
	return fmt.Errorf("C runtime %s: %s (%d)", operation, C.GoString(C.trevrpc_error(code)), int(code))
}
