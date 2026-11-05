package trevrpc

import (
	"errors"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/ipv4"
)

// A UDP load balancer cannot open a connection with a PROXY header, so the
// ones in front of the UDP socket put a version 2 header in front of every
// datagram they forward instead; see ListenOptions.TrustedUDPProxies. The
// socket strips it before quic-go reads the datagram, so QUIC keeps talking to
// the load balancer, and remembers the client it names for the address the
// datagram came from. The server reports that client as the remote address of
// the connection.

const (
	// proxiedFlowTimeout is how long the client of an address is remembered
	// after its last datagram. Every datagram names the client again and the
	// server looks it up as the streams and requests of a datagram arrive, so
	// it only has to outlast their processing.
	proxiedFlowTimeout = time.Minute
	// proxiedFlowSweepInterval is how often the addresses whose datagrams
	// stopped are forgotten.
	proxiedFlowSweepInterval = 30 * time.Second
	// maxProxiedFlows bounds how many addresses of trusted proxies are
	// remembered. Datagrams from further addresses are dropped.
	maxProxiedFlows = 1 << 18
	// proxyDropReportInterval is how often dropped datagrams are reported.
	proxyDropReportInterval = 10 * time.Second
)

var errTooManyProxiedFlows = errors.New("too many addresses of trusted UDP proxies")

// proxiedClients maps the addresses that trusted UDP proxies forward
// datagrams from to the clients their PROXY headers name.
type proxiedClients struct {
	mu      sync.RWMutex
	clients map[netip.AddrPort]netip.AddrPort
}

// clientOf returns the client the datagrams from peer were last forwarded
// for, or peer itself when their headers name none.
func (p *proxiedClients) clientOf(peer netip.AddrPort) netip.AddrPort {
	p.mu.RLock()
	client, ok := p.clients[peer]
	p.mu.RUnlock()
	if ok {
		return client
	}
	return peer
}

// apply records changes; one without a client forgets its address.
func (p *proxiedClients) apply(changes []proxiedFlowChange) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, change := range changes {
		if change.client.IsValid() {
			p.clients[change.peer] = change.client
		} else {
			delete(p.clients, change.peer)
		}
	}
}

// proxiedUDPConn is the UDP socket of a Listener behind trusted UDP proxies.
// It strips the PROXY header of every datagram from them, and drops the ones
// without a valid header. Datagrams from anywhere else are not read for one.
//
// quic-go reads with ReadBatch on Linux and macOS and with ReadFrom elsewhere;
// the other read methods of the embedded *net.UDPConn do not strip headers
// and must not be used. Writes, socket options and GSO detection go to the
// UDP socket unchanged.
type proxiedUDPConn struct {
	*net.UDPConn
	batch   *ipv4.PacketConn
	trusted []netip.Prefix
	clients *proxiedClients
	report  func(ServerDiagnostic)

	// mu guards what follows. quic-go reads from one goroutine, so it is not
	// contended.
	mu        sync.Mutex
	flows     map[netip.AddrPort]*proxiedFlow
	changes   []proxiedFlowChange
	lastSweep time.Time
	// dropped counts the datagrams dropped since the last report, the last
	// of them from dropPeer for dropErr.
	dropped    int
	dropErr    error
	dropPeer   netip.AddrPort
	lastReport time.Time
}

// proxiedFlow is what the socket knows of one address of a trusted proxy.
type proxiedFlow struct {
	// client is the client the last header named, or the zero AddrPort when
	// it named none.
	client netip.AddrPort
	seen   time.Time
}

type proxiedFlowChange struct {
	peer, client netip.AddrPort
}

func newProxiedUDPConn(udp *net.UDPConn, trusted []netip.Prefix, report func(ServerDiagnostic)) *proxiedUDPConn {
	return &proxiedUDPConn{
		UDPConn: udp,
		batch:   ipv4.NewPacketConn(udp),
		trusted: trusted,
		clients: &proxiedClients{clients: make(map[netip.AddrPort]netip.AddrPort)},
		report:  report,
		flows:   make(map[netip.AddrPort]*proxiedFlow),
	}
}

// ReadBatch reads datagrams as quic-go reads them from a *net.UDPConn, and
// strips their headers. A dropped datagram stays in the batch with no bytes,
// which quic-go skips, so that the messages keep their buffers.
func (c *proxiedUDPConn) ReadBatch(ms []ipv4.Message, flags int) (int, error) {
	n, err := c.batch.ReadBatch(ms, flags)
	if n > 0 {
		c.mu.Lock()
		var now time.Time
		for i := range ms[:n] {
			message := &ms[i]
			if len(message.Buffers) == 0 {
				continue
			}
			buffer := message.Buffers[0]
			message.N = c.strip(buffer[:min(message.N, len(buffer))], message.Addr, &now)
		}
		report := c.finish(now)
		c.mu.Unlock()
		c.emit(report)
	}
	return n, err
}

// ReadFrom reads the next datagram that is not dropped, without its header.
func (c *proxiedUDPConn) ReadFrom(b []byte) (int, net.Addr, error) {
	for {
		n, addr, err := c.UDPConn.ReadFrom(b)
		if err != nil {
			return n, addr, err
		}
		if n = c.stripOne(b[:n], addr); n > 0 {
			return n, addr, nil
		}
	}
}

// ReadMsgUDP reads the next datagram that is not dropped, without its header.
func (c *proxiedUDPConn) ReadMsgUDP(b, oob []byte) (n, oobn, flags int, addr *net.UDPAddr, err error) {
	for {
		n, oobn, flags, addr, err = c.UDPConn.ReadMsgUDP(b, oob)
		if err != nil {
			return n, oobn, flags, addr, err
		}
		if n = c.stripOne(b[:n], addr); n > 0 {
			return n, oobn, flags, addr, nil
		}
	}
}

// stripOne strips the header of one datagram, as strip does.
func (c *proxiedUDPConn) stripOne(b []byte, addr net.Addr) int {
	c.mu.Lock()
	var now time.Time
	n := c.strip(b, addr, &now)
	report := c.finish(now)
	c.mu.Unlock()
	c.emit(report)
	return n
}

// strip removes the PROXY header from b, the datagram from addr, records the
// client it names, and returns the length of what follows it. It returns 0
// for a datagram to drop, and b's length for one from an untrusted address.
// now is set to the time the first datagram from a trusted proxy arrived.
func (c *proxiedUDPConn) strip(b []byte, addr net.Addr, now *time.Time) int {
	peer := addrPortOf(addr)
	if !trustedProxyIP(c.trusted, peer.Addr()) {
		return len(b)
	}
	if now.IsZero() {
		*now = time.Now()
	}
	client, length, err := parseProxyV2Datagram(b)
	if err == nil && !c.track(peer, client, *now) {
		err = errTooManyProxiedFlows
	}
	if err != nil {
		c.dropped++
		c.dropErr, c.dropPeer = err, peer
		return 0
	}
	// A datagram with only a header, such as a health check, has no bytes
	// left and is dropped.
	return copy(b, b[length:])
}

// track records that client sent the datagram from peer that arrived at now.
// It returns false when peer is new and too many addresses are known.
func (c *proxiedUDPConn) track(peer, client netip.AddrPort, now time.Time) bool {
	flow, ok := c.flows[peer]
	switch {
	case !ok:
		if len(c.flows) >= maxProxiedFlows {
			return false
		}
		flow = &proxiedFlow{client: client}
		c.flows[peer] = flow
		if client.IsValid() {
			c.changes = append(c.changes, proxiedFlowChange{peer: peer, client: client})
		}
	case flow.client != client:
		flow.client = client
		c.changes = append(c.changes, proxiedFlowChange{peer: peer, client: client})
	}
	flow.seen = now
	return true
}

// finish ends a read whose first datagram from a trusted proxy arrived at
// now, if any: it forgets the addresses that went quiet, publishes the clients
// that changed, and returns the report of dropped datagrams that is due.
func (c *proxiedUDPConn) finish(now time.Time) *ServerDiagnostic {
	if now.IsZero() {
		return nil
	}
	if now.Sub(c.lastSweep) >= proxiedFlowSweepInterval {
		for peer, flow := range c.flows {
			if now.Sub(flow.seen) > proxiedFlowTimeout {
				delete(c.flows, peer)
				if flow.client.IsValid() {
					c.changes = append(c.changes, proxiedFlowChange{peer: peer})
				}
			}
		}
		c.lastSweep = now
	}
	if len(c.changes) > 0 {
		c.clients.apply(c.changes)
		clear(c.changes)
		c.changes = c.changes[:0]
	}
	if c.dropped == 0 || now.Sub(c.lastReport) < proxyDropReportInterval {
		return nil
	}
	report := &ServerDiagnostic{
		Phase:      ServerDiagnosticProxyHeaderFailed,
		Message:    "dropped " + strconv.Itoa(c.dropped) + " datagrams from trusted UDP proxies",
		Err:        c.dropErr,
		RemoteAddr: c.dropPeer.String(),
	}
	c.dropped, c.dropErr, c.lastReport = 0, nil, now
	return report
}

// emit reports report, if any, outside the lock.
func (c *proxiedUDPConn) emit(report *ServerDiagnostic) {
	if report != nil && c.report != nil {
		c.report(*report)
	}
}

// parseProxyV2Datagram parses the version 2 header that must begin b and
// returns the client address it carries, or the zero AddrPort for a LOCAL
// command or an address family other than UDP over IPv4 or IPv6, and the
// header's length. Type-length-value extensions are skipped.
func parseProxyV2Datagram(b []byte) (netip.AddrPort, int, error) {
	addressLength, length, err := parseProxyV2Fixed(b, proxyV2Datagram)
	if err != nil {
		return netip.AddrPort{}, 0, err
	}
	if length > len(b) {
		return netip.AddrPort{}, 0, errProxyHeader
	}
	var source netip.AddrPort
	if addressLength > 0 {
		source = proxyV2Source(b[proxyV2HeaderLength : proxyV2HeaderLength+addressLength])
	}
	return source, length, nil
}
