package ratelimit

import (
	"context"
	"net/netip"
	"testing"
	"time"

	trevrpc "trev.zip/llc/trevrpc/trevrpc-go"
)

// clientInfos returns the contexts of calls to method from n clients.
func clientInfos(method string, n int, v6 bool) []trevrpc.HandlerContext {
	infos := make([]trevrpc.HandlerContext, n)
	for i := range infos {
		addr := netip.AddrFrom4([4]byte{10, byte(i >> 16), byte(i >> 8), byte(i)})
		if v6 {
			addr = netip.AddrFrom16([16]byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, byte(i >> 8), byte(i), 0, 0, 0, 0, 0, 0, 0, 1})
		}
		infos[i] = trevrpc.HandlerContext{FullMethod: method, RemoteAddr: netip.AddrPortFrom(addr, 4433)}
	}
	return infos
}

// benchmarkIntercept measures Intercept for calls that spread over clients,
// from every CPU at once, as a busy server's connections do. The policy is
// generous enough that no call is rejected.
func benchmarkIntercept(b *testing.B, options Options, infos []trevrpc.HandlerContext) {
	l := New(options)
	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, _, err := l.Intercept(ctx, infos[i%len(infos)]); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}

var generous = &Policy{Requests: 1 << 30, Window: 1 << 30}

// BenchmarkInterceptUnlimitedMethod measures a call to a method with no policy,
// which costs a map lookup.
func BenchmarkInterceptUnlimitedMethod(b *testing.B) {
	benchmarkIntercept(b, Options{Policies: map[string]Policy{loginMethod: *generous}}, clientInfos(searchMethod, 1000, false))
}

// BenchmarkInterceptIPv4 measures a call of a known IPv4 client.
func BenchmarkInterceptIPv4(b *testing.B) {
	benchmarkIntercept(b, Options{Default: generous}, clientInfos(searchMethod, 1000, false))
}

// BenchmarkInterceptIPv6 measures a call of a known IPv6 client, whose address
// is grouped into its prefix.
func BenchmarkInterceptIPv6(b *testing.B) {
	benchmarkIntercept(b, Options{Default: generous}, clientInfos(searchMethod, 1000, true))
}

// BenchmarkInterceptOneClient measures calls of a single client, which all
// contend for one bucket.
func BenchmarkInterceptOneClient(b *testing.B) {
	benchmarkIntercept(b, Options{Default: generous}, clientInfos(searchMethod, 1, false))
}

// BenchmarkInterceptKey measures a call with a key function, which returns
// text the call looks the bucket up with.
func BenchmarkInterceptKey(b *testing.B) {
	infos := clientInfos(searchMethod, 1000, false)
	for i := range infos {
		infos[i].Metadata = trevrpc.Metadata{"x-api-key": []byte{byte(i >> 8), byte(i)}}
	}
	key := func(info trevrpc.HandlerContext) string { return string(info.Metadata["x-api-key"]) }
	benchmarkIntercept(b, Options{Default: generous, Key: key}, infos)
}

// BenchmarkInterceptEvicting measures calls of more clients than the limiter
// keeps buckets for, so that every call replaces the oldest bucket.
func BenchmarkInterceptEvicting(b *testing.B) {
	benchmarkIntercept(b, Options{Default: generous, MaxBuckets: 100}, clientInfos(searchMethod, 100_000, false))
}

// BenchmarkInterceptRejecting measures a call that finds its bucket empty,
// which builds a status.
func BenchmarkInterceptRejecting(b *testing.B) {
	l := New(Options{Default: &Policy{Requests: 1, Window: time.Hour}})
	info := clientInfos(searchMethod, 1, false)[0]
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if _, _, err := l.Intercept(ctx, info); err == nil && b.N < 0 {
			b.Fatal("unreachable")
		}
	}
}
