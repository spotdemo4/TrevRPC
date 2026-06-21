package main

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"strings"
	"unicode"

	"google.golang.org/protobuf/proto"
	descriptor "google.golang.org/protobuf/types/descriptorpb"
	plugin "google.golang.org/protobuf/types/pluginpb"
)

type pluginOptions struct {
	headerSuffix   string
	sourceSuffix   string
	runtimeInclude string
}

type typeRef struct {
	protoName string
	cType     string
	cPrefix   string
}

type typeIndex map[string]typeRef

type methodInfo struct {
	name            string
	cName           string
	input           typeRef
	output          typeRef
	clientStreaming bool
	serverStreaming bool
}

func main() {
	if err := run(os.Stdin, os.Stdout); err != nil {
		fmt.Fprintf(os.Stderr, "protoc-gen-trevrpc-c: %v\n", err)
		os.Exit(1)
	}
}

func run(reader io.Reader, writer io.Writer) error {
	input, err := io.ReadAll(reader)
	if err != nil {
		return err
	}

	request := &plugin.CodeGeneratorRequest{}
	if err := proto.Unmarshal(input, request); err != nil {
		return fmt.Errorf("decode CodeGeneratorRequest: %w", err)
	}

	response := generate(request)
	output, err := proto.Marshal(response)
	if err != nil {
		return fmt.Errorf("encode CodeGeneratorResponse: %w", err)
	}

	_, err = writer.Write(output)
	return err
}

func defaultPluginOptions() pluginOptions {
	return pluginOptions{
		headerSuffix:   ".trevrpc.h",
		sourceSuffix:   ".trevrpc.c",
		runtimeInclude: "trevrpc.h",
	}
}

func parseOptions(parameter string) (pluginOptions, error) {
	options := defaultPluginOptions()
	if parameter == "" {
		return options, nil
	}

	for option := range strings.SplitSeq(parameter, ",") {
		if option == "" {
			continue
		}
		key, value, ok := strings.Cut(option, "=")
		if !ok {
			return options, fmt.Errorf("invalid trevrpc-c option %q; expected key=value", option)
		}
		switch key {
		case "header_suffix":
			options.headerSuffix = value
		case "source_suffix":
			options.sourceSuffix = value
		case "runtime_include":
			options.runtimeInclude = value
		default:
			return options, fmt.Errorf("unknown trevrpc-c option %q", key)
		}
	}

	return options, nil
}

func generate(request *plugin.CodeGeneratorRequest) *plugin.CodeGeneratorResponse {
	options, err := parseOptions(request.GetParameter())
	if err != nil {
		return &plugin.CodeGeneratorResponse{Error: new(err.Error())}
	}

	filesToGenerate := map[string]bool{}
	for _, fileName := range request.FileToGenerate {
		filesToGenerate[fileName] = true
	}

	index := buildTypeIndex(request.ProtoFile)
	response := &plugin.CodeGeneratorResponse{}
	for _, file := range request.ProtoFile {
		if !filesToGenerate[file.GetName()] || len(file.Service) == 0 {
			continue
		}

		header, source, err := generateFile(file, index, options)
		if err != nil {
			return &plugin.CodeGeneratorResponse{Error: new(err.Error())}
		}

		response.File = append(response.File,
			&plugin.CodeGeneratorResponse_File{Name: new(outputFileName(file.GetName(), options.headerSuffix)), Content: new(header)},
			&plugin.CodeGeneratorResponse_File{Name: new(outputFileName(file.GetName(), options.sourceSuffix)), Content: new(source)},
		)
	}

	return response
}

func buildTypeIndex(files []*descriptor.FileDescriptorProto) typeIndex {
	index := typeIndex{}
	for _, file := range files {
		for _, message := range file.MessageType {
			indexMessage(index, file.GetPackage(), nil, message)
		}
	}
	return index
}

func indexMessage(index typeIndex, protoPackage string, parents []string, message *descriptor.DescriptorProto) {
	parts := append(append([]string{}, strings.Split(protoPackage, ".")...), append(parents, message.GetName())...)
	if protoPackage == "" {
		parts = append(append([]string{}, parents...), message.GetName())
	}
	protoName := fullProtoName(protoPackage, append(parents, message.GetName()))
	index[protoName] = typeRef{
		protoName: protoName,
		cType:     protobufCType(parts),
		cPrefix:   protobufCPrefix(parts),
	}
	for _, nested := range message.NestedType {
		indexMessage(index, protoPackage, append(parents, message.GetName()), nested)
	}
}

func generateFile(file *descriptor.FileDescriptorProto, index typeIndex, options pluginOptions) (string, string, error) {
	services := make([]serviceInfo, 0, len(file.Service))
	for _, service := range file.Service {
		info, err := describeService(file, service, index)
		if err != nil {
			return "", "", err
		}
		services = append(services, info)
	}

	header := generateHeader(file, services, options)
	source := generateSource(file, services, options)
	return header, source, nil
}

type serviceInfo struct {
	protoName string
	cName     string
	typeName  string
	methods   []methodInfo
	inputs    []typeRef
	outputs   []typeRef
}

func describeService(file *descriptor.FileDescriptorProto, service *descriptor.ServiceDescriptorProto, index typeIndex) (serviceInfo, error) {
	serviceParts := cNamespaceParts(file.GetPackage(), service.GetName())
	info := serviceInfo{
		protoName: service.GetName(),
		cName:     strings.Join(toSnakeParts(serviceParts), "_"),
		typeName:  strings.Join(toSnakeParts(serviceParts), "_") + "_server",
	}
	if file.GetPackage() != "" {
		info.protoName = file.GetPackage() + "." + service.GetName()
	}

	inputSeen := map[string]bool{}
	outputSeen := map[string]bool{}
	for _, method := range service.Method {
		methodInfo, err := describeMethod(file, method, index)
		if err != nil {
			return serviceInfo{}, err
		}
		info.methods = append(info.methods, methodInfo)
		if !inputSeen[methodInfo.input.protoName] {
			inputSeen[methodInfo.input.protoName] = true
			info.inputs = append(info.inputs, methodInfo.input)
		}
		if !outputSeen[methodInfo.output.protoName] {
			outputSeen[methodInfo.output.protoName] = true
			info.outputs = append(info.outputs, methodInfo.output)
		}
	}

	sort.Slice(info.inputs, func(i, j int) bool { return info.inputs[i].cType < info.inputs[j].cType })
	sort.Slice(info.outputs, func(i, j int) bool { return info.outputs[i].cType < info.outputs[j].cType })
	return info, nil
}

func describeMethod(file *descriptor.FileDescriptorProto, method *descriptor.MethodDescriptorProto, index typeIndex) (methodInfo, error) {
	input, err := cTypeFor(file, method.GetInputType(), index)
	if err != nil {
		return methodInfo{}, err
	}
	output, err := cTypeFor(file, method.GetOutputType(), index)
	if err != nil {
		return methodInfo{}, err
	}

	return methodInfo{
		name:            method.GetName(),
		cName:           toSnake(method.GetName()),
		input:           input,
		output:          output,
		clientStreaming: method.GetClientStreaming(),
		serverStreaming: method.GetServerStreaming(),
	}, nil
}

func generateHeader(file *descriptor.FileDescriptorProto, services []serviceInfo, options pluginOptions) string {
	guard := headerGuard(outputFileName(file.GetName(), options.headerSuffix))
	var buffer bytes.Buffer
	fmt.Fprintf(&buffer, "#ifndef %s\n#define %s\n\n", guard, guard)
	buffer.WriteString("#include <stdint.h>\n")
	buffer.WriteString("#include <protobuf-c/protobuf-c.h>\n")
	fmt.Fprintf(&buffer, "#include %q\n", options.runtimeInclude)
	fmt.Fprintf(&buffer, "#include %q\n\n", protobufCHeader(file.GetName()))
	buffer.WriteString("#ifdef __cplusplus\nextern \"C\" {\n#endif\n\n")

	for _, service := range services {
		generateHeaderService(&buffer, service)
	}

	buffer.WriteString("#ifdef __cplusplus\n}\n#endif\n\n")
	fmt.Fprintf(&buffer, "#endif\n")
	return buffer.String()
}

func generateHeaderService(buffer *bytes.Buffer, service serviceInfo) {
	fmt.Fprintf(buffer, "typedef struct %s {\n", service.typeName)
	buffer.WriteString("    void* user_data;\n")
	for _, method := range service.methods {
		switch {
		case !method.clientStreaming && !method.serverStreaming:
			fmt.Fprintf(buffer, "    int (*%s)(void* user_data, const trevrpc_call_context* context, const %s* request, %s** response);\n", method.cName, method.input.cType, method.output.cType)
		case method.clientStreaming && !method.serverStreaming:
			fmt.Fprintf(buffer, "    int (*%s)(void* user_data, const trevrpc_call_context* context, trevrpc_stream* stream, %s** response);\n", method.cName, method.output.cType)
		case !method.clientStreaming && method.serverStreaming:
			fmt.Fprintf(buffer, "    int (*%s)(void* user_data, const trevrpc_call_context* context, const %s* request, trevrpc_stream* stream);\n", method.cName, method.input.cType)
		default:
			fmt.Fprintf(buffer, "    int (*%s)(void* user_data, const trevrpc_call_context* context, trevrpc_stream* stream);\n", method.cName)
		}
	}
	fmt.Fprintf(buffer, "} %s;\n\n", service.typeName)

	for _, input := range service.inputs {
		fmt.Fprintf(buffer, "int %s_send_%s(trevrpc_stream* stream, const %s* message);\n", service.cName, typeHelperName(input), input.cType)
	}
	for _, output := range service.outputs {
		fmt.Fprintf(buffer, "int %s_recv_%s(trevrpc_stream* stream, %s** message, uint32_t* status);\n", service.cName, typeHelperName(output), output.cType)
	}
	buffer.WriteByte('\n')

	for _, method := range service.methods {
		switch {
		case !method.clientStreaming && !method.serverStreaming:
			fmt.Fprintf(buffer, "int %s_%s(trevrpc_client* client, const %s* request, %s** response);\n", service.cName, method.cName, method.input.cType, method.output.cType)
		case method.clientStreaming:
			fmt.Fprintf(buffer, "int %s_%s_start(trevrpc_client* client, trevrpc_stream** stream);\n", service.cName, method.cName)
		case method.serverStreaming:
			fmt.Fprintf(buffer, "int %s_%s(trevrpc_client* client, const %s* request, trevrpc_stream** stream);\n", service.cName, method.cName, method.input.cType)
		}
	}
	fmt.Fprintf(buffer, "int %s_register(trevrpc_server* server, const %s* implementation);\n\n", service.cName, service.typeName)
}

func generateSource(file *descriptor.FileDescriptorProto, services []serviceInfo, options pluginOptions) string {
	var buffer bytes.Buffer
	buffer.WriteString("// Code generated by protoc-gen-trevrpc-c. DO NOT EDIT.\n\n")
	buffer.WriteString("#include <stdlib.h>\n")
	buffer.WriteString("#include <string.h>\n")
	fmt.Fprintf(&buffer, "#include %q\n\n", path.Base(outputFileName(file.GetName(), options.headerSuffix)))

	for _, service := range services {
		generateSourceService(&buffer, service)
	}
	return buffer.String()
}

func generateSourceService(buffer *bytes.Buffer, service serviceInfo) {
	for _, input := range service.inputs {
		generateSendHelper(buffer, service, input)
	}
	for _, output := range service.outputs {
		generateRecvHelper(buffer, service, output)
	}
	for _, method := range service.methods {
		generateClientWrapper(buffer, service, method)
		generateServerCallback(buffer, service, method)
	}
	generateRegisterFunction(buffer, service)
}

func generatePackCall(message typeRef, value string) string {
	return fmt.Sprintf("%s__get_packed_size(%s)", message.cPrefix, value)
}

func generateSendHelper(buffer *bytes.Buffer, service serviceInfo, message typeRef) {
	fmt.Fprintf(buffer, "int %s_send_%s(trevrpc_stream* stream, const %s* message) {\n", service.cName, typeHelperName(message), message.cType)
	buffer.WriteString("    if (message == NULL) { return -22; }\n")
	fmt.Fprintf(buffer, "    size_t body_len = %s;\n", generatePackCall(message, "message"))
	buffer.WriteString("    uint8_t* body = body_len == 0 ? NULL : malloc(body_len);\n")
	buffer.WriteString("    if (body_len > 0 && body == NULL) { return -12; }\n")
	fmt.Fprintf(buffer, "    %s__pack(message, body);\n", message.cPrefix)
	buffer.WriteString("    int err = trevrpc_stream_send_message(stream, body, body_len);\n")
	buffer.WriteString("    free(body);\n")
	buffer.WriteString("    return err;\n")
	buffer.WriteString("}\n\n")
}

func generateRecvHelper(buffer *bytes.Buffer, service serviceInfo, message typeRef) {
	fmt.Fprintf(buffer, "int %s_recv_%s(trevrpc_stream* stream, %s** message, uint32_t* status) {\n", service.cName, typeHelperName(message), message.cType)
	buffer.WriteString("    if (message == NULL) { return -22; }\n")
	buffer.WriteString("    *message = NULL;\n")
	buffer.WriteString("    if (status != NULL) { *status = TREVRPC_STATUS_OK; }\n")
	buffer.WriteString("    trevrpc_stream_frame* frame = NULL;\n")
	buffer.WriteString("    int err = trevrpc_stream_recv(stream, &frame);\n")
	buffer.WriteString("    if (err != 0 || frame == NULL) { return err; }\n")
	buffer.WriteString("    if (frame->kind == TREVRPC_STREAM_FRAME_KIND_STATUS) {\n")
	buffer.WriteString("        if (status != NULL) { *status = frame->status; }\n")
	buffer.WriteString("        trevrpc_stream_frame_free(frame);\n")
	buffer.WriteString("        return 0;\n")
	buffer.WriteString("    }\n")
	fmt.Fprintf(buffer, "    *message = %s__unpack(NULL, frame->body_len, frame->body);\n", message.cPrefix)
	buffer.WriteString("    trevrpc_stream_frame_free(frame);\n")
	buffer.WriteString("    return *message == NULL ? TREVRPC_ERR_INVALID_FRAME : 0;\n")
	buffer.WriteString("}\n\n")
}

func generateClientWrapper(buffer *bytes.Buffer, service serviceInfo, method methodInfo) {
	switch {
	case !method.clientStreaming && !method.serverStreaming:
		fmt.Fprintf(buffer, "int %s_%s(trevrpc_client* client, const %s* request, %s** response) {\n", service.cName, method.cName, method.input.cType, method.output.cType)
		buffer.WriteString("    if (request == NULL || response == NULL) { return -22; }\n")
		buffer.WriteString("    *response = NULL;\n")
		writePackRequest(buffer, method.input, "request")
		fmt.Fprintf(buffer, "    trevrpc_response* rpc_response = NULL;\n    int err = trevrpc_client_call_unary(client, %q, %q, body, body_len, &rpc_response);\n", service.protoName, method.name)
		buffer.WriteString("    free(body);\n")
		buffer.WriteString("    if (err != 0) { return err; }\n")
		buffer.WriteString("    if (rpc_response->status != TREVRPC_STATUS_OK) { err = (int)rpc_response->status; trevrpc_response_free(rpc_response); return err; }\n")
		fmt.Fprintf(buffer, "    *response = %s__unpack(NULL, rpc_response->body_len, rpc_response->body);\n", method.output.cPrefix)
		buffer.WriteString("    trevrpc_response_free(rpc_response);\n")
		buffer.WriteString("    return *response == NULL ? TREVRPC_ERR_INVALID_FRAME : 0;\n")
		buffer.WriteString("}\n\n")
	case method.clientStreaming:
		kind := "TREVRPC_RPC_KIND_CLIENT_STREAMING"
		if method.serverStreaming {
			kind = "TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING"
		}
		fmt.Fprintf(buffer, "int %s_%s_start(trevrpc_client* client, trevrpc_stream** stream) {\n", service.cName, method.cName)
		fmt.Fprintf(buffer, "    return trevrpc_client_start_stream(client, %q, %q, %s, NULL, 0, stream);\n", service.protoName, method.name, kind)
		buffer.WriteString("}\n\n")
	case method.serverStreaming:
		fmt.Fprintf(buffer, "int %s_%s(trevrpc_client* client, const %s* request, trevrpc_stream** stream) {\n", service.cName, method.cName, method.input.cType)
		buffer.WriteString("    if (request == NULL || stream == NULL) { return -22; }\n")
		writePackRequest(buffer, method.input, "request")
		fmt.Fprintf(buffer, "    int err = trevrpc_client_start_stream(client, %q, %q, TREVRPC_RPC_KIND_SERVER_STREAMING, body, body_len, stream);\n", service.protoName, method.name)
		buffer.WriteString("    free(body);\n")
		buffer.WriteString("    if (err == 0) { err = trevrpc_stream_finish_send(*stream); }\n")
		buffer.WriteString("    return err;\n")
		buffer.WriteString("}\n\n")
	}
}

func writePackRequest(buffer *bytes.Buffer, message typeRef, name string) {
	fmt.Fprintf(buffer, "    size_t body_len = %s;\n", generatePackCall(message, name))
	buffer.WriteString("    uint8_t* body = body_len == 0 ? NULL : malloc(body_len);\n")
	buffer.WriteString("    if (body_len > 0 && body == NULL) { return -12; }\n")
	fmt.Fprintf(buffer, "    %s__pack(%s, body);\n", message.cPrefix, name)
}

func generateServerCallback(buffer *bytes.Buffer, service serviceInfo, method methodInfo) {
	callbackName := fmt.Sprintf("%s_%s_callback", service.cName, method.cName)
	if !method.clientStreaming && !method.serverStreaming {
		fmt.Fprintf(buffer, "static int %s(void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_response* response) {\n", callbackName)
		fmt.Fprintf(buffer, "    const %s* implementation = (const %s*)user_data;\n", service.typeName, service.typeName)
		fmt.Fprintf(buffer, "    %s* decoded = %s__unpack(NULL, request->body_len, request->body);\n", method.input.cType, method.input.cPrefix)
		buffer.WriteString("    if (decoded == NULL) { return TREVRPC_ERR_INVALID_FRAME; }\n")
		fmt.Fprintf(buffer, "    %s* reply = NULL;\n", method.output.cType)
		fmt.Fprintf(buffer, "    int err = implementation->%s(implementation->user_data, context, decoded, &reply);\n", method.cName)
		fmt.Fprintf(buffer, "    %s__free_unpacked(decoded, NULL);\n", method.input.cPrefix)
		buffer.WriteString("    if (err != 0) { return err; }\n")
		buffer.WriteString("    if (reply == NULL) { return TREVRPC_ERR_HANDLER_FAILED; }\n")
		writePackRequest(buffer, method.output, "reply")
		buffer.WriteString("    err = trevrpc_response_set_body(response, body, body_len);\n")
		buffer.WriteString("    free(body);\n")
		buffer.WriteString("    return err;\n")
		buffer.WriteString("}\n\n")
		return
	}

	fmt.Fprintf(buffer, "static int %s(void* user_data, const trevrpc_call_context* context, const trevrpc_request* request, trevrpc_stream* stream) {\n", callbackName)
	fmt.Fprintf(buffer, "    const %s* implementation = (const %s*)user_data;\n", service.typeName, service.typeName)
	switch {
	case method.clientStreaming && method.serverStreaming:
		fmt.Fprintf(buffer, "    (void)request;\n")
		fmt.Fprintf(buffer, "    return implementation->%s(implementation->user_data, context, stream);\n", method.cName)
	case method.clientStreaming:
		fmt.Fprintf(buffer, "    %s* reply = NULL;\n", method.output.cType)
		fmt.Fprintf(buffer, "    (void)request;\n")
		fmt.Fprintf(buffer, "    int err = implementation->%s(implementation->user_data, context, stream, &reply);\n", method.cName)
		buffer.WriteString("    if (err != 0) { return err; }\n")
		buffer.WriteString("    if (reply == NULL) { return TREVRPC_ERR_HANDLER_FAILED; }\n")
		writePackRequest(buffer, method.output, "reply")
		buffer.WriteString("    err = trevrpc_stream_send_message(stream, body, body_len);\n")
		buffer.WriteString("    free(body);\n")
		buffer.WriteString("    return err;\n")
	case method.serverStreaming:
		fmt.Fprintf(buffer, "    %s* decoded = %s__unpack(NULL, request->body_len, request->body);\n", method.input.cType, method.input.cPrefix)
		buffer.WriteString("    if (decoded == NULL) { return TREVRPC_ERR_INVALID_FRAME; }\n")
		fmt.Fprintf(buffer, "    int err = implementation->%s(implementation->user_data, context, decoded, stream);\n", method.cName)
		fmt.Fprintf(buffer, "    %s__free_unpacked(decoded, NULL);\n", method.input.cPrefix)
		buffer.WriteString("    return err;\n")
	}
	buffer.WriteString("}\n\n")
}

func generateRegisterFunction(buffer *bytes.Buffer, service serviceInfo) {
	fmt.Fprintf(buffer, "int %s_register(trevrpc_server* server, const %s* implementation) {\n", service.cName, service.typeName)
	buffer.WriteString("    if (implementation == NULL) { return -22; }\n")
	for _, method := range service.methods {
		callbackName := fmt.Sprintf("%s_%s_callback", service.cName, method.cName)
		if !method.clientStreaming && !method.serverStreaming {
			fmt.Fprintf(buffer, "    if (implementation->%s == NULL) { return -22; }\n", method.cName)
			fmt.Fprintf(buffer, "    int err_%s = trevrpc_server_register_unary(server, %q, %q, %s, (void*)implementation);\n", method.cName, service.protoName, method.name, callbackName)
		} else {
			kind := "TREVRPC_RPC_KIND_SERVER_STREAMING"
			if method.clientStreaming && method.serverStreaming {
				kind = "TREVRPC_RPC_KIND_BIDIRECTIONAL_STREAMING"
			} else if method.clientStreaming {
				kind = "TREVRPC_RPC_KIND_CLIENT_STREAMING"
			}
			fmt.Fprintf(buffer, "    if (implementation->%s == NULL) { return -22; }\n", method.cName)
			fmt.Fprintf(buffer, "    int err_%s = trevrpc_server_register_streaming(server, %q, %q, %s, %s, (void*)implementation);\n", method.cName, service.protoName, method.name, kind, callbackName)
		}
		fmt.Fprintf(buffer, "    if (err_%s != 0) { return err_%s; }\n", method.cName, method.cName)
	}
	buffer.WriteString("    return 0;\n")
	buffer.WriteString("}\n\n")
}

func cTypeFor(file *descriptor.FileDescriptorProto, protoName string, index typeIndex) (typeRef, error) {
	if !strings.HasPrefix(protoName, ".") {
		protoName = "." + file.GetPackage() + "." + protoName
	}
	ref, ok := index[protoName]
	if !ok {
		return typeRef{}, fmt.Errorf("unknown protobuf message type %q", protoName)
	}
	return ref, nil
}

func outputFileName(inputName, suffix string) string {
	ext := path.Ext(inputName)
	return strings.TrimSuffix(inputName, ext) + suffix
}

func fullProtoName(protoPackage string, names []string) string {
	parts := make([]string, 0, len(names)+1)
	if protoPackage != "" {
		parts = append(parts, protoPackage)
	}
	parts = append(parts, names...)
	return "." + strings.Join(parts, ".")
}

func cNamespaceParts(protoPackage, name string) []string {
	parts := []string{}
	if protoPackage != "" {
		parts = append(parts, strings.Split(protoPackage, ".")...)
	}
	parts = append(parts, name)
	return parts
}

func protobufCType(parts []string) string {
	cParts := make([]string, 0, len(parts))
	for _, part := range parts {
		cParts = append(cParts, toCamel(part))
	}
	return strings.Join(cParts, "__")
}

func protobufCPrefix(parts []string) string {
	cParts := make([]string, 0, len(parts))
	for _, part := range parts {
		cParts = append(cParts, toSnake(part))
	}
	return strings.Join(cParts, "__")
}

func toSnakeParts(parts []string) []string {
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		out = append(out, toSnake(part))
	}
	return out
}

func typeHelperName(typeRef typeRef) string {
	parts := strings.Split(typeRef.cPrefix, "__")
	return strings.Join(parts, "_")
}

func protobufCHeader(inputName string) string {
	ext := path.Ext(inputName)
	return path.Base(strings.TrimSuffix(inputName, ext) + ".pb-c.h")
}

func headerGuard(name string) string {
	var builder strings.Builder
	for _, r := range strings.ToUpper(name) {
		if r == '_' || unicode.IsLetter(r) || unicode.IsDigit(r) {
			builder.WriteRune(r)
		} else {
			builder.WriteByte('_')
		}
	}
	return builder.String()
}

func toCamel(name string) string {
	parts := strings.FieldsFunc(name, func(r rune) bool { return r == '_' || r == '-' || r == '.' })
	for i, part := range parts {
		if part == "" {
			continue
		}
		runes := []rune(part)
		runes[0] = unicode.ToUpper(runes[0])
		parts[i] = string(runes)
	}
	return strings.Join(parts, "")
}

func toSnake(name string) string {
	var builder strings.Builder
	var prevLower bool
	for i, r := range name {
		if r == '-' || r == '.' {
			if builder.Len() > 0 && builder.String()[builder.Len()-1] != '_' {
				builder.WriteByte('_')
			}
			prevLower = false
			continue
		}
		if r == '_' {
			if builder.Len() > 0 && builder.String()[builder.Len()-1] != '_' {
				builder.WriteByte('_')
			}
			prevLower = false
			continue
		}
		if unicode.IsUpper(r) && prevLower && i > 0 {
			builder.WriteByte('_')
		}
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			builder.WriteRune(unicode.ToLower(r))
			prevLower = unicode.IsLower(r) || unicode.IsDigit(r)
		}
	}
	result := strings.Trim(builder.String(), "_")
	if result == "" {
		return "x"
	}
	if unicode.IsDigit([]rune(result)[0]) {
		return "_" + result
	}
	return result
}
