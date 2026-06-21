//go:build trevrpc_msquic_native && cgo

package trevrpc

/*
#cgo CFLAGS: -I${SRCDIR}/../trevrpc-c/include -I${SRCDIR}/../trevrpc-c/src
#cgo LDFLAGS: -lpthread
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include "trevrpc.c"

static int trevrpc_c_echo_handler(void* user_data, const trevrpc_request* request, trevrpc_response* response) {
    (void)user_data;
    return trevrpc_response_set_body(response, request->body, request->body_len);
}

static int trevrpc_c_register_echo(trevrpc_server* server) {
    return trevrpc_server_register_unary(server, "test.EchoService", "Echo", trevrpc_c_echo_handler, NULL);
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

type cRuntimeNativeServer struct {
	ptr     *C.trevrpc_server
	thread  C.pthread_t
	started bool
}

func startCRuntimeNativeEchoServer(host, certFile, keyFile string, port uint16) (*cRuntimeNativeServer, error) {
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
		return nil, cRuntimeNativeError("listen", code)
	}

	if code := C.trevrpc_c_register_echo(server); code != 0 {
		C.trevrpc_server_close(server)
		return nil, cRuntimeNativeError("register unary handler", code)
	}

	runtimeServer := &cRuntimeNativeServer{ptr: server}
	if code := C.trevrpc_c_server_start(server, &runtimeServer.thread); code != 0 {
		C.trevrpc_server_close(server)
		return nil, cRuntimeNativeError("start serve thread", code)
	}
	runtimeServer.started = true
	return runtimeServer, nil
}

func (s *cRuntimeNativeServer) close() error {
	if s == nil || s.ptr == nil {
		return nil
	}

	C.trevrpc_server_shutdown(s.ptr)
	var err error
	if s.started {
		if code := C.trevrpc_c_server_join(s.thread); code != 0 {
			err = cRuntimeNativeError("join serve thread", code)
		}
		s.started = false
	}
	C.trevrpc_server_close(s.ptr)
	s.ptr = nil
	return err
}

func cRuntimeNativeCallUnary(host string, port uint16, service, method string, body []byte) ([]byte, uint32, error) {
	cHost := C.CString(host)
	defer C.free(unsafe.Pointer(cHost))
	cService := C.CString(service)
	defer C.free(unsafe.Pointer(cService))
	cMethod := C.CString(method)
	defer C.free(unsafe.Pointer(cMethod))

	clientConfig := C.trevrpc_default_config()
	var client *C.trevrpc_client
	if code := C.trevrpc_client_connect(cHost, C.uint16_t(port), &clientConfig, &client); code != 0 {
		return nil, 0, cRuntimeNativeError("connect client", code)
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
		return nil, 0, cRuntimeNativeError("call unary", code)
	}
	defer C.trevrpc_response_free(response)

	var responseBody []byte
	if response.body_len > 0 {
		responseBody = C.GoBytes(unsafe.Pointer(response.body), C.int(response.body_len))
	}
	return responseBody, uint32(response.status), nil
}

func cRuntimeNativeError(operation string, code C.int) error {
	return fmt.Errorf("C runtime %s: %s (%d)", operation, C.GoString(C.trevrpc_error(code)), int(code))
}
