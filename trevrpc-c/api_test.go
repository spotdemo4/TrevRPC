package trevrpcc_test

import (
	"reflect"
	"testing"

	trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"
)

func TestDefaultConfigurations(t *testing.T) {
	engine := trevrpcc.DefaultEngineConfig()
	transport := trevrpcc.DefaultTransportConfig()
	if engine.EventCapacity != trevrpcc.DefaultEventCapacity || transport.EventCapacity != engine.EventCapacity {
		t.Fatalf("unexpected default capacities: Engine %+v, Transport %+v", engine, transport)
	}
	endpoint := trevrpcc.DefaultEndpointConfig()
	if endpoint.Protocol != trevrpcc.ProtocolNative || endpoint.WebTransportProfiles != trevrpcc.WebTransportProfileAllSupported {
		t.Fatalf("unexpected endpoint defaults: %+v", endpoint)
	}
}

func TestPublicRuntimeContractsContainOnlyGoTypes(t *testing.T) {
	roots := []reflect.Type{
		reflect.TypeOf((*trevrpcc.Provider)(nil)).Elem(),
		reflect.TypeOf((*trevrpcc.EngineRuntime)(nil)).Elem(),
		reflect.TypeOf((*trevrpcc.TransportRuntime)(nil)).Elem(),
		reflect.TypeOf(trevrpcc.EndpointConfig{}),
		reflect.TypeOf(trevrpcc.Event{}),
	}
	seen := map[reflect.Type]bool{}
	for _, root := range roots {
		assertPureGoType(t, root, seen)
	}
}

func assertPureGoType(t *testing.T, value reflect.Type, seen map[reflect.Type]bool) {
	t.Helper()
	if value == nil || seen[value] {
		return
	}
	seen[value] = true
	if value.Kind() == reflect.UnsafePointer || value.PkgPath() == "C" {
		t.Fatalf("public contract contains non-Go type %v", value)
	}
	switch value.Kind() {
	case reflect.Array, reflect.Chan, reflect.Pointer, reflect.Slice:
		assertPureGoType(t, value.Elem(), seen)
	case reflect.Func:
		for index := 0; index < value.NumIn(); index++ {
			assertPureGoType(t, value.In(index), seen)
		}
		for index := 0; index < value.NumOut(); index++ {
			assertPureGoType(t, value.Out(index), seen)
		}
	case reflect.Interface:
		for index := 0; index < value.NumMethod(); index++ {
			assertPureGoType(t, value.Method(index).Type, seen)
		}
	case reflect.Map:
		assertPureGoType(t, value.Key(), seen)
		assertPureGoType(t, value.Elem(), seen)
	case reflect.Struct:
		for index := 0; index < value.NumField(); index++ {
			if value.Field(index).IsExported() {
				assertPureGoType(t, value.Field(index).Type, seen)
			}
		}
	}
}
