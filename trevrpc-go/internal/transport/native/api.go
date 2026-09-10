package native

import trevrpcc "trev.zip/llc/trevrpc/trevrpc-c"

const (
	EngineABIVersion    = trevrpcc.EngineABIVersion
	TransportABIVersion = trevrpcc.TransportABIVersion

	WakeSourcePOSIXFD      = trevrpcc.WakeSourcePOSIXFD
	WakeFlagBorrowed       = trevrpcc.WakeFlagBorrowed
	WakeFlagLevelTriggered = trevrpcc.WakeFlagLevelTriggered
)

var ErrUnavailable = trevrpcc.ErrUnavailable

type WakeSource = trevrpcc.WakeSource

type StatusError = trevrpcc.StatusError
