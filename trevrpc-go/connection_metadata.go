package trevrpc

import (
	"fmt"
	"maps"
	"sync"
	"sync/atomic"
)

// Connection metadata is request metadata a client sets once for all its calls,
// such as a bearer token, so that it crosses each connection once instead of in
// every REQUEST. A client offers the set (RpcRequest.connection_metadata with
// entries) on its calls until a HEADERS confirms that the server stored it for
// the connection, then refers to it by ID. Calls on different streams reach
// the server in any order, so a reference only follows a confirmation. The
// server merges the set into each call's metadata before validation and
// authorization, the call's own entries winning; see the wiki's Connection
// Metadata section.

// maxConnectionMetadataSets is how many sets a server keeps per connection:
// the newest, and a few the client's calls in flight may still refer to.
const maxConnectionMetadataSets = 4

// connectionMetadataSet is one version of a channel's connection metadata.
type connectionMetadataSet struct {
	id       uint32
	metadata Metadata
}

// connectionMetadataSource is a channel's connection metadata, shared by its
// connections: the current set, whose ID changes with it.
type connectionMetadataSource struct {
	current atomic.Pointer[connectionMetadataSet]
	ids     atomic.Uint32
}

// newConnectionMetadataSource returns the source of a channel whose
// connection metadata starts as metadata.
func newConnectionMetadataSource(metadata Metadata) (*connectionMetadataSource, error) {
	source := &connectionMetadataSource{}
	if err := source.set(metadata); err != nil {
		return nil, err
	}
	return source, nil
}

// set replaces the connection metadata; empty metadata removes it.
func (s *connectionMetadataSource) set(metadata Metadata) error {
	if len(metadata) == 0 {
		s.current.Store(nil)
		return nil
	}
	normalized := make(Metadata, len(metadata))
	for key, value := range metadata {
		normalized[NormalizeMetadataKey(key)] = append([]byte(nil), value...)
	}
	if err := validateMetadata(normalized); err != nil {
		return err
	}
	s.current.Store(&connectionMetadataSet{id: s.ids.Add(1), metadata: normalized})
	return nil
}

// clientConnectionMetadata is a connection's view of its channel's connection
// metadata: the set the server confirmed on this connection.
type clientConnectionMetadata struct {
	source    *connectionMetadataSource
	confirmed atomic.Uint32
}

// newClientConnectionMetadata returns the state of a new connection, or nil
// for a channel without connection metadata.
func newClientConnectionMetadata(source *connectionMetadataSource) *clientConnectionMetadata {
	if source == nil {
		return nil
	}
	return &clientConnectionMetadata{source: source}
}

// prepare sets the connection metadata of request: a reference to the current
// set once the connection confirmed it, an offer of it otherwise. It returns
// the ID it offered, which learn checks the HEADERS against, or zero.
func (c *clientConnectionMetadata) prepare(request *wireRequest) uint32 {
	if c == nil {
		return 0
	}
	set := c.source.current.Load()
	if set == nil {
		return 0
	}
	if c.confirmed.Load() == set.id {
		request.ConnectionMetadata = &connectionMetadataInfo{ID: set.id}
		return 0
	}
	request.ConnectionMetadata = &connectionMetadataInfo{ID: set.id, Metadata: set.metadata}
	return set.id
}

// learn records that the server stored the offered set, once its HEADERS
// confirm it.
func (c *clientConnectionMetadata) learn(offered uint32, answer headersAnswer) {
	if c != nil && offered != 0 && answer.connectionMetadata == offered {
		c.confirmed.Store(offered)
	}
}

// connectionMetadataStore is the connection metadata a server stored for one
// connection, a QMux session or a WebTransport session.
type connectionMetadataStore struct {
	mu   sync.Mutex
	sets []connectionMetadataSet
}

// connectionMetadataStream is implemented by the server streams of
// connections that keep connection metadata. Others, the ordinary HTTP
// mapping's, use an offer for its own call only and know no references.
type connectionMetadataStream interface {
	trevrpcConnectionMetadata() *connectionMetadataStore
}

// streamConnectionMetadata returns the store of the connection stream belongs
// to, or nil.
func streamConnectionMetadata(stream rpcStream) *connectionMetadataStore {
	if connection, ok := stream.(connectionMetadataStream); ok {
		return connection.trevrpcConnectionMetadata()
	}
	return nil
}

// resolve merges the connection metadata request offers or refers to into its
// metadata, the call's own entries winning, and stores an offered set for the
// connection, which the call's HEADERS then confirm. s is nil on a connection
// that keeps none.
func (s *connectionMetadataStore) resolve(request *wireRequest) error {
	info := request.ConnectionMetadata
	if info == nil {
		return nil
	}
	set := info.Metadata
	if len(set) > 0 {
		if err := validateMetadata(set); err != nil {
			return err
		}
		if s != nil {
			s.store(connectionMetadataSet{id: info.ID, metadata: set})
			request.connectionMetadataConfirmed = info.ID
		}
	} else {
		var ok bool
		if set, ok = s.lookup(info.ID); !ok {
			return InvalidArgument(fmt.Sprintf("unknown connection metadata set %d", info.ID))
		}
	}
	if request.Metadata == nil {
		request.Metadata = make(Metadata, len(set))
	}
	for key, value := range set {
		if _, ok := request.Metadata[key]; !ok {
			// A copy: the connection's later calls share the stored values,
			// and the handler owns its request metadata.
			request.Metadata[key] = append([]byte(nil), value...)
		}
	}
	return nil
}

// store keeps set, replacing one with the same ID and dropping the oldest
// beyond maxConnectionMetadataSets.
func (s *connectionMetadataStore) store(set connectionMetadataSet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, stored := range s.sets {
		if stored.id == set.id {
			s.sets = append(s.sets[:i], s.sets[i+1:]...)
			break
		}
	}
	if len(s.sets) == maxConnectionMetadataSets {
		s.sets = append(s.sets[:0], s.sets[1:]...)
	}
	s.sets = append(s.sets, connectionMetadataSet{id: set.id, metadata: maps.Clone(set.metadata)})
}

// lookup returns the stored set id.
func (s *connectionMetadataStore) lookup(id uint32) (Metadata, bool) {
	if s == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, stored := range s.sets {
		if stored.id == id {
			return stored.metadata, true
		}
	}
	return nil, false
}
