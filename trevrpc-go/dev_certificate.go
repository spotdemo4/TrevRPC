package trevrpc

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// developmentCertificateLifetime is how long each development certificate
	// is valid. Browsers trust a certificate by hash only when it is valid for
	// at most 14 days.
	developmentCertificateLifetime = 13 * 24 * time.Hour
	// developmentCertificatePeriod is how long each development certificate is
	// presented before the next replaces it, which leaves it a week of validity
	// for the connections that verified it.
	developmentCertificatePeriod = 6 * 24 * time.Hour
	// developmentCertificateBackdate starts each certificate's validity before
	// it is first presented, for clients whose clocks are behind.
	developmentCertificateBackdate = time.Hour
)

// defaultDevelopmentHosts are the names a DevelopmentCertificate covers when
// none are given.
var defaultDevelopmentHosts = []string{"localhost", "127.0.0.1", "::1"}

// DevelopmentCertificate is a self-signed ECDSA P-256 certificate for local
// development that rotates itself, so that a server works without any
// certificate setup and browsers can trust it through the WebTransport
// serverCertificateHashes option, which accepts only certificates valid for
// at most 14 days.
//
// Each certificate is valid for 13 days and presented for 6, and the next one
// is generated a period ahead. Hashes and the JSON that ServeHTTP answers list
// both, so a page that fetched them before a rotation still trusts the
// certificate presented after it. Production servers use a certificate from a
// certificate authority instead.
type DevelopmentCertificate struct {
	hosts []string
	// now returns the current time; tests replace it.
	now   func() time.Time
	state atomic.Pointer[developmentCertificates]
	// mu serializes rotations.
	mu sync.Mutex
}

// developmentCertificates are the certificate being presented and the one
// that replaces it.
type developmentCertificates struct {
	current, next *developmentCertificate
}

type developmentCertificate struct {
	tls  tls.Certificate
	hash [sha256.Size]byte
	pem  []byte
	// presentUntil is when the next certificate replaces this one.
	presentUntil time.Time
}

// NewDevelopmentCertificate generates a development certificate for hosts,
// which are DNS names or IP addresses: localhost, 127.0.0.1 and ::1 when
// empty.
func NewDevelopmentCertificate(hosts ...string) (*DevelopmentCertificate, error) {
	return newDevelopmentCertificate(time.Now, hosts...)
}

func newDevelopmentCertificate(now func() time.Time, hosts ...string) (*DevelopmentCertificate, error) {
	if len(hosts) == 0 {
		hosts = defaultDevelopmentHosts
	}
	c := &DevelopmentCertificate{hosts: append([]string(nil), hosts...), now: now}
	certificates, err := c.generatePair(c.now())
	if err != nil {
		return nil, err
	}
	c.state.Store(certificates)
	return c, nil
}

// TLSConfig returns a TLS configuration that presents the current
// certificate. Listen uses it for a server with
// ServerOptions.DevelopmentCertificate, so ListenOptions.TLSConfig is only
// needed to present the certificate elsewhere.
func (c *DevelopmentCertificate) TLSConfig() *tls.Config {
	return &tls.Config{GetCertificate: c.GetCertificate}
}

// GetCertificate returns the current certificate, rotating it first when its
// period is over. It has the signature of tls.Config.GetCertificate.
func (c *DevelopmentCertificate) GetCertificate(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	certificates, err := c.certificates()
	if err != nil {
		return nil, err
	}
	return &certificates.current.tls, nil
}

// Hashes returns the SHA-256 hashes of the current and the next certificate,
// for serverCertificateHashes.
func (c *DevelopmentCertificate) Hashes() ([][sha256.Size]byte, error) {
	certificates, err := c.certificates()
	if err != nil {
		return nil, err
	}
	return [][sha256.Size]byte{certificates.current.hash, certificates.next.hash}, nil
}

// PEM returns the current and the next certificate PEM-encoded, for native
// clients to trust as roots. A client that loads them keeps trusting the
// server across one rotation.
func (c *DevelopmentCertificate) PEM() ([]byte, error) {
	certificates, err := c.certificates()
	if err != nil {
		return nil, err
	}
	return append(append([]byte(nil), certificates.current.pem...), certificates.next.pem...), nil
}

// developmentCertificateHash is one entry of the JSON that ServeHTTP answers,
// shaped like a WebTransport serverCertificateHashes entry with a hex value.
type developmentCertificateHash struct {
	Algorithm string `json:"algorithm"`
	Value     string `json:"value"`
}

// certificateHashesSuffix follows ServerOptions.HTTPPath in the path where a
// server publishes the hashes of its DevelopmentCertificate.
const certificateHashesSuffix = "/certificate-hashes"

// setCertificateHashesCORS lets pages of any origin read a response with the
// certificate hashes, as a development web server such as Vite serves the page
// from another origin than the TrevRPC server. The hashes are no secret: any
// client that connects sees the certificate. The header is on every response,
// 404 included, which tells the page that the browser trusts the certificate
// already.
func setCertificateHashesCORS(header http.Header) {
	header.Set("Access-Control-Allow-Origin", "*")
}

// certificateHashesNotFound answers the hashes path of a server without a
// DevelopmentCertificate.
var certificateHashesNotFound = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	setCertificateHashesCORS(w.Header())
	http.NotFound(w, r)
})

// ServeHTTP answers GET and HEAD requests with the certificate hashes as JSON,
// such as [{"algorithm":"sha-256","value":"<hex>"}], which the JavaScript
// client reads with fetchServerCertificateHashes. Responses allow any origin
// to read them.
//
// A server with ServerOptions.DevelopmentCertificate answers this itself at
// HTTPPath + "/certificate-hashes". Mount the certificate as a handler only to
// publish the hashes elsewhere, such as on the server that serves the web app,
// and point the client's serverCertificateHashesUrl option at it.
func (c *DevelopmentCertificate) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	setCertificateHashesCORS(w.Header())
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	hashes, err := c.Hashes()
	if err != nil {
		http.Error(w, "development certificate unavailable", http.StatusInternalServerError)
		return
	}
	entries := make([]developmentCertificateHash, len(hashes))
	for i, hash := range hashes {
		entries[i] = developmentCertificateHash{Algorithm: "sha-256", Value: hex.EncodeToString(hash[:])}
	}
	body, err := json.Marshal(entries)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(body)
}

// certificates returns the certificates for the current time, rotating them
// when the current one's period is over. A failed rotation keeps the old
// certificates for the next attempt.
func (c *DevelopmentCertificate) certificates() (*developmentCertificates, error) {
	now := c.now()
	if certificates := c.state.Load(); now.Before(certificates.current.presentUntil) {
		return certificates, nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	certificates := c.state.Load()
	if now.Before(certificates.current.presentUntil) {
		return certificates, nil
	}
	if now.Before(certificates.next.presentUntil) {
		next, err := c.generate(certificates.next.presentUntil)
		if err != nil {
			return nil, err
		}
		certificates = &developmentCertificates{current: certificates.next, next: next}
	} else {
		// Both periods are over, as after the machine slept: start afresh.
		pair, err := c.generatePair(now)
		if err != nil {
			return nil, err
		}
		certificates = pair
	}
	c.state.Store(certificates)
	return certificates, nil
}

// generatePair generates a certificate presented from now and the one after it.
func (c *DevelopmentCertificate) generatePair(now time.Time) (*developmentCertificates, error) {
	current, err := c.generate(now)
	if err != nil {
		return nil, err
	}
	next, err := c.generate(current.presentUntil)
	if err != nil {
		return nil, err
	}
	return &developmentCertificates{current: current, next: next}, nil
}

// generate generates a certificate presented from presentFrom for one period.
func (c *DevelopmentCertificate) generate(presentFrom time.Time) (*developmentCertificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, Internal("generate development certificate key: " + err.Error())
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, Internal("generate development certificate serial number: " + err.Error())
	}
	notBefore := presentFrom.Add(-developmentCertificateBackdate)
	template := &x509.Certificate{
		SerialNumber: serial,
		NotBefore:    notBefore,
		NotAfter:     notBefore.Add(developmentCertificateLifetime),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	for _, host := range c.hosts {
		if ip := net.ParseIP(host); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, host)
		}
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, Internal("create development certificate: " + err.Error())
	}
	leaf, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, Internal("parse development certificate: " + err.Error())
	}
	return &developmentCertificate{
		tls:          tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: leaf},
		hash:         sha256.Sum256(der),
		pem:          pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		presentUntil: presentFrom.Add(developmentCertificatePeriod),
	}, nil
}
