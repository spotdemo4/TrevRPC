//go:build trevrpc_msquic_native && cgo

package cruntimetest

/*
#cgo pkg-config: msquic
#cgo CFLAGS: -I${SRCDIR}/../../../trevrpc-c/include -I${SRCDIR}/../../../trevrpc-c/src
#cgo LDFLAGS: -lpthread
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include "trevrpc_msquic.c"
#include "trevrpc.c"

static int trevrpc_c_echo_handler(void* user_data, const trevrpc_request* request, trevrpc_response* response) {
    (void)user_data;
    return trevrpc_response_set_body(response, request->body, request->body_len);
}

static int trevrpc_c_server_stream_handler(void* user_data, const trevrpc_request* request, trevrpc_stream* stream) {
    (void)user_data;
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
*/
import "C"

import (
	"fmt"
	"unsafe"
)

const statusOK = 0
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

	var server *C.trevrpc_server
	if code := C.trevrpc_server_listen(cHost, C.uint16_t(port), &serverConfig, &server); code != 0 {
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
