package transport

import (
	"net"
	"testing"
)

func TestAddressFromNetSeparatesHostOnlyIPv6Zone(t *testing.T) {
	address := AddressFromNet(&net.IPAddr{
		IP:   net.ParseIP("fe80::1"),
		Zone: "eth0",
	})
	if address.Host != "fe80::1" || address.Zone != "eth0" || address.Port != 0 {
		t.Fatalf("AddressFromNet() = %+v", address)
	}
	if got := address.String(); got != "fe80::1%eth0" {
		t.Fatalf("Address.String() = %q", got)
	}
}
