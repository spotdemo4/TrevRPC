package trevrpc

// PlainCompression marks the directions of a method whose messages may be
// compressed with zstd one at a time without a dictionary: a ZSTD_MESSAGE with
// no Dictionary_ID, where no dictionary the peers share applies to the message
// type. Generated code passes the method's schema option, compress (which
// compress_stream implies), with a direction left out when its message type
// has a field marked debug_redact; hand-written code can pass it to
// WithPlainCompression (clients) and RoutePlainCompression (servers).
//
// Messages of plainCompressionMinSize bytes or more are compressed, and sent
// so only when that makes them smaller. Peers negotiate it per connection as
// the dictionary with ID 0, so a peer that does not decode such messages keeps
// receiving MESSAGE frames: responses may use it from a connection's first
// call, requests once the server's answer to that call has arrived. On a
// stream-compressed direction, the stream context takes over from its third
// message. It calls libzstd and needs cgo; without cgo it is off.
//
// As with any compression, the compressed size of a message that mixes data an
// attacker can influence with secrets can reveal the secrets, so do not mark
// such methods.
type PlainCompression struct {
	// Requests lets the request messages be compressed without a dictionary.
	Requests bool
	// Responses lets the response messages be compressed without a dictionary.
	Responses bool
}

// plainCompressionMinSize is the smallest message compressed without a
// dictionary: smaller ones rarely shrink on their own.
const plainCompressionMinSize = 256

// WithPlainCompression lets the call compress the directions p marks without a
// dictionary; see PlainCompression. Generated clients pass the method's schema
// option.
func WithPlainCompression(p PlainCompression) CallOption {
	return func(config *callConfig) {
		config.plainCompression = p
	}
}

// RoutePlainCompression lets the route's calls compress the response messages
// without a dictionary if p.Responses is set; see PlainCompression. Servers
// accept such request messages on every call, so p.Requests has no effect.
// Generated registration passes the method's schema option.
func RoutePlainCompression(p PlainCompression) RouteOption {
	return func(config *routeConfig) {
		config.plainCompression = p
	}
}
