package trevrpc

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"
)

// fakeClock is a settable time source for development certificate tests.
type fakeClock struct{ now time.Time }

func (c *fakeClock) Now() time.Time { return c.now }

func newTestDevelopmentCertificate(t *testing.T, hosts ...string) (*DevelopmentCertificate, *fakeClock) {
	t.Helper()
	clock := &fakeClock{now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	certificate, err := newDevelopmentCertificate(clock.Now, hosts...)
	if err != nil {
		t.Fatalf("newDevelopmentCertificate: %v", err)
	}
	return certificate, clock
}

func currentLeaf(t *testing.T, certificate *DevelopmentCertificate) *x509.Certificate {
	t.Helper()
	presented, err := certificate.GetCertificate(nil)
	if err != nil {
		t.Fatalf("GetCertificate: %v", err)
	}
	return presented.Leaf
}

func mustHashes(t *testing.T, certificate *DevelopmentCertificate) [][sha256.Size]byte {
	t.Helper()
	hashes, err := certificate.Hashes()
	if err != nil {
		t.Fatalf("Hashes: %v", err)
	}
	return hashes
}

func TestDevelopmentCertificateCoversHostsWithinTheBrowserLimit(t *testing.T) {
	certificate, clock := newTestDevelopmentCertificate(t)
	leaf := currentLeaf(t, certificate)
	if got, want := leaf.DNSNames, []string{"localhost"}; !slices.Equal(got, want) {
		t.Fatalf("DNSNames = %v, want %v", got, want)
	}
	if got := len(leaf.IPAddresses); got != 2 || !leaf.IPAddresses[0].Equal(net.IPv4(127, 0, 0, 1)) || !leaf.IPAddresses[1].Equal(net.IPv6loopback) {
		t.Fatalf("IPAddresses = %v, want 127.0.0.1 and ::1", leaf.IPAddresses)
	}
	if key, ok := leaf.PublicKey.(*ecdsa.PublicKey); !ok || key.Curve != elliptic.P256() {
		t.Fatalf("public key = %T, want ECDSA P-256", leaf.PublicKey)
	}
	if lifetime := leaf.NotAfter.Sub(leaf.NotBefore); lifetime > 14*24*time.Hour {
		t.Fatalf("validity = %v, more than browsers accept by hash", lifetime)
	}
	if !leaf.NotBefore.Before(clock.now) {
		t.Fatalf("NotBefore = %v, not before the time it is first presented", leaf.NotBefore)
	}

	custom, _ := newTestDevelopmentCertificate(t, "dev.example.test")
	if got := currentLeaf(t, custom).DNSNames; !slices.Equal(got, []string{"dev.example.test"}) {
		t.Fatalf("custom DNSNames = %v", got)
	}
}

func TestDevelopmentCertificateRotatesToTheAdvertisedNextCertificate(t *testing.T) {
	certificate, clock := newTestDevelopmentCertificate(t)
	first := mustHashes(t, certificate)
	if len(first) != 2 || first[0] == first[1] {
		t.Fatalf("Hashes = %x, want the current and a different next certificate", first)
	}
	if got := sha256.Sum256(currentLeaf(t, certificate).Raw); got != first[0] {
		t.Fatal("the first hash is not the presented certificate's")
	}

	clock.now = clock.now.Add(developmentCertificatePeriod - time.Second)
	if got := mustHashes(t, certificate); !slices.Equal(got, first) {
		t.Fatal("rotated before the period ended")
	}

	clock.now = clock.now.Add(time.Second)
	leaf := currentLeaf(t, certificate)
	if got := sha256.Sum256(leaf.Raw); got != first[1] {
		t.Fatal("the certificate presented after a rotation is not the advertised next one")
	}
	if !leaf.NotBefore.Before(clock.now) || !clock.now.Add(developmentCertificatePeriod).Before(leaf.NotAfter) {
		t.Fatalf("the next certificate is valid from %v to %v, not over its period from %v", leaf.NotBefore, leaf.NotAfter, clock.now)
	}
	second := mustHashes(t, certificate)
	if second[0] != first[1] || second[1] == first[0] || second[1] == first[1] {
		t.Fatalf("Hashes after a rotation = %x, want the old next certificate and a new one", second)
	}
}

func TestDevelopmentCertificateStartsAfreshAfterBothPeriods(t *testing.T) {
	certificate, clock := newTestDevelopmentCertificate(t)
	first := mustHashes(t, certificate)
	clock.now = clock.now.Add(3 * developmentCertificatePeriod)
	leaf := currentLeaf(t, certificate)
	if hash := sha256.Sum256(leaf.Raw); slices.Contains(first, hash) {
		t.Fatal("presented a certificate from before the gap")
	}
	if !leaf.NotBefore.Before(clock.now) || !clock.now.Before(leaf.NotAfter) {
		t.Fatalf("certificate valid from %v to %v, not now (%v)", leaf.NotBefore, leaf.NotAfter, clock.now)
	}
}

func TestDevelopmentCertificateServesItsHashesAsJSON(t *testing.T) {
	certificate, _ := newTestDevelopmentCertificate(t)
	recorder := httptest.NewRecorder()
	certificate.ServeHTTP(recorder, httptest.NewRequestWithContext(context.Background(), http.MethodGet, "/certificate-hashes", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d", recorder.Code)
	}
	if got := recorder.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("Content-Type = %q", got)
	}
	var entries []struct{ Algorithm, Value string }
	if err := json.Unmarshal(recorder.Body.Bytes(), &entries); err != nil {
		t.Fatalf("body %q: %v", recorder.Body, err)
	}
	hashes := mustHashes(t, certificate)
	if len(entries) != len(hashes) {
		t.Fatalf("entries = %+v, want %d", entries, len(hashes))
	}
	for i, entry := range entries {
		if entry.Algorithm != "sha-256" || entry.Value != hex.EncodeToString(hashes[i][:]) {
			t.Fatalf("entry %d = %+v, want sha-256 %x", i, entry, hashes[i])
		}
	}

	recorder = httptest.NewRecorder()
	certificate.ServeHTTP(recorder, httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/certificate-hashes", nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", recorder.Code)
	}
}

func TestDevelopmentCertificateServesNativeClientsThatTrustItsPEM(t *testing.T) {
	certificate, err := NewDevelopmentCertificate()
	if err != nil {
		t.Fatal(err)
	}
	certificatePEM, err := certificate.PEM()
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(certificatePEM) {
		t.Fatal("PEM holds no certificates")
	}
	pki := testPKI{server: certificate.TLSConfig(), client: &tls.Config{RootCAs: roots, ServerName: "localhost"}}
	listener := startTestListener(t, pki, nil)
	channel := dialNative(t, pki, listener)

	ctx, cancel := context.WithTimeout(context.Background(), rpcTestTimeout)
	defer cancel()
	reply, err := Unary(ctx, channel, "/example.Greeter/Echo", &testMessage{Value: "hi"}, newTestMessage)
	if err != nil {
		t.Fatalf("Echo: %v", err)
	}
	if reply.Value != "hi" {
		t.Fatalf("Echo = %q", reply.Value)
	}
}
