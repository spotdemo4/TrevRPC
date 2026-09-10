package trevrpcc

// Engine is an adopted provider Engine whose C storage remains private to its owner.
type Engine interface {
	Identity() uint64
	Close() error
}

// Transport is an adopted provider Transport whose C storage remains private to its owner.
type Transport interface {
	Identity() uint64
	Close() error
}
