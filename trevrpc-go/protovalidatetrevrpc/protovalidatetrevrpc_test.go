package protovalidatetrevrpc

import (
	"strings"
	"testing"

	"buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"trev.zip/llc/trevrpc/trevrpc-go"
)

func fieldRules(rules *validate.FieldRules) *descriptorpb.FieldOptions {
	options := &descriptorpb.FieldOptions{}
	proto.SetExtension(options, validate.E_Field, rules)
	return options
}

func field(name string, number int32, typ descriptorpb.FieldDescriptorProto_Type, typeName string, options *descriptorpb.FieldOptions) *descriptorpb.FieldDescriptorProto {
	label := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	field := &descriptorpb.FieldDescriptorProto{Name: &name, Number: &number, Type: typ.Enum(), Label: &label, Options: options}
	if typeName != "" {
		field.TypeName = &typeName
	}
	return field
}

func repeatedField(name string, number int32, typ descriptorpb.FieldDescriptorProto_Type, options *descriptorpb.FieldOptions) *descriptorpb.FieldDescriptorProto {
	repeated := field(name, number, typ, "", options)
	repeated.Label = descriptorpb.FieldDescriptorProto_LABEL_REPEATED.Enum()
	return repeated
}

// testFile describes, in package test:
//
//	message Request { string name = 1 [min_len 3]; Inner inner = 2; }
//	message Inner { int32 count = 1 [gt 0]; }
//	message Outer { Holder holder = 1; }
//	message Holder { Inner inner = 1; }
//	message Plain { string name = 1; Plain next = 2; }
//	message BadRule { string name = 1 [cel "this +"]; }
//	message Items { repeated string items = 1 [items min_len 1]; }
func testFile(t *testing.T) protoreflect.FileDescriptor {
	t.Helper()
	str, msg, i32 := descriptorpb.FieldDescriptorProto_TYPE_STRING, descriptorpb.FieldDescriptorProto_TYPE_MESSAGE, descriptorpb.FieldDescriptorProto_TYPE_INT32
	message := func(name string, fields ...*descriptorpb.FieldDescriptorProto) *descriptorpb.DescriptorProto {
		return &descriptorpb.DescriptorProto{Name: new(name), Field: fields}
	}
	file := &descriptorpb.FileDescriptorProto{
		Name:       new("test.proto"),
		Package:    new("test"),
		Syntax:     new("proto3"),
		Dependency: []string{"buf/validate/validate.proto"},
		MessageType: []*descriptorpb.DescriptorProto{
			message("Request",
				field("name", 1, str, "", fieldRules(validate.FieldRules_builder{String: validate.StringRules_builder{MinLen: proto.Uint64(3)}.Build()}.Build())),
				field("inner", 2, msg, ".test.Inner", nil)),
			message("Inner", field("count", 1, i32, "", fieldRules(validate.FieldRules_builder{Int32: validate.Int32Rules_builder{Gt: proto.Int32(0)}.Build()}.Build()))),
			message("Outer", field("holder", 1, msg, ".test.Holder", nil)),
			message("Holder", field("inner", 1, msg, ".test.Inner", nil)),
			message("Plain", field("name", 1, str, "", nil), field("next", 2, msg, ".test.Plain", nil)),
			message("BadRule", field("name", 1, str, "", fieldRules(validate.FieldRules_builder{Cel: []*validate.Rule{validate.Rule_builder{Id: new("bad"), Expression: new("this +")}.Build()}}.Build()))),
			message("Items", repeatedField("items", 1, str, fieldRules(validate.FieldRules_builder{Repeated: validate.RepeatedRules_builder{Items: validate.FieldRules_builder{String: validate.StringRules_builder{MinLen: proto.Uint64(1)}.Build()}.Build()}.Build()}.Build()))),
		},
	}
	descriptor, err := protodesc.NewFile(file, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}

func TestTypesWithoutRulesAreNotChecked(t *testing.T) {
	file := testFile(t)
	validator := New(nil)
	for name, wantRules := range map[string]bool{"Request": true, "Inner": true, "Outer": true, "Holder": true, "Plain": false} {
		validate, err := validator.ValidateFunc(file.Messages().ByName(protoreflect.Name(name)))
		if err != nil || (validate != nil) != wantRules {
			t.Errorf("ValidateFunc(%s) = %v, %v; want a function: %v", name, validate != nil, err, wantRules)
		}
	}
}

func TestBrokenRulesFailWithViolations(t *testing.T) {
	desc := testFile(t).Messages().ByName("Request")
	validateRequest, err := New(nil).ValidateFunc(desc)
	if err != nil {
		t.Fatal(err)
	}

	request := dynamicpb.NewMessage(desc)
	request.Set(desc.Fields().ByName("name"), protoreflect.ValueOfString("ok!"))
	if err := validateRequest(request); err != nil {
		t.Fatalf("valid request: %v", err)
	}

	request.Set(desc.Fields().ByName("name"), protoreflect.ValueOfString("no"))
	inner := request.Mutable(desc.Fields().ByName("inner")).Message()
	inner.Set(inner.Descriptor().Fields().ByName("count"), protoreflect.ValueOfInt32(-1))
	status := trevrpc.StatusFromError(validateRequest(request))
	if status.Code != trevrpc.CodeInvalidArgument {
		t.Fatalf("status = %v, want InvalidArgument", status)
	}
	violations := &validate.Violations{}
	if found, err := status.FindDetail(violations); !found || err != nil {
		t.Fatalf("FindDetail = %v, %v; want buf.validate.Violations", found, err)
	}
	var rules []string
	for _, violation := range violations.GetViolations() {
		rules = append(rules, violation.GetRuleId())
	}
	if strings.Join(rules, ",") != "string.min_len,int32.gt" {
		t.Fatalf("violated rules = %v, want string.min_len and int32.gt", rules)
	}
}

func TestRulesThatCannotCompileFailToBind(t *testing.T) {
	desc := testFile(t).Messages().ByName("BadRule")
	if validate, err := New(nil).ValidateFunc(desc); err == nil || validate != nil {
		t.Fatalf("ValidateFunc = %v, %v; want the compilation error", validate != nil, err)
	}
}

func TestManyViolationsFailWithAStatusThatFitsAFrame(t *testing.T) {
	desc := testFile(t).Messages().ByName("Items")
	validateItems, err := New(nil).ValidateFunc(desc)
	if err != nil {
		t.Fatal(err)
	}
	request := dynamicpb.NewMessage(desc)
	items := request.Mutable(desc.Fields().ByName("items")).List()
	for range 5000 {
		items.Append(protoreflect.ValueOfString(""))
	}

	status := trevrpc.StatusFromError(validateItems(request))
	if status.Code != trevrpc.CodeInvalidArgument || len(status.Message) > 200 || !strings.HasSuffix(status.Message, "(and 4999 more)") {
		t.Fatalf("status = %v %q, want InvalidArgument naming the first violation and counting the rest", status.Code, status.Message)
	}
	violations := &validate.Violations{}
	if found, err := status.FindDetail(violations); !found || err != nil || len(violations.GetViolations()) != MaxDetailViolations {
		t.Fatalf("detail: found %v, %v, %d violations; want %d", found, err, len(violations.GetViolations()), MaxDetailViolations)
	}
}
