package trevrpc

import (
	"errors"
	"math"
	"slices"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
)

// retryInfoType is the full name of google.rpc.RetryInfo, the standard status
// detail that tells a client when to retry. TrevRPC encodes and decodes the
// message itself, which is two nested varints, so that it does not depend on
// googleapis; the encoding is the one every protobuf library produces.
const retryInfoType = "google.rpc.RetryInfo"

// RetryInfo field numbers: RetryInfo.retry_delay is a google.protobuf.Duration
// with seconds and nanos.
const (
	retryInfoDelayField  protowire.Number = 1
	durationSecondsField protowire.Number = 1
	durationNanosField   protowire.Number = 2
)

// RetryInfoDetail returns the google.rpc.RetryInfo detail that tells a client
// to wait delay before it retries, for a Status built by hand; WithRetryDelay
// attaches one to an existing status. A negative delay is zero.
func RetryInfoDetail(delay time.Duration) Detail {
	return Detail{Type: retryInfoType, Value: appendRetryInfo(nil, delay)}
}

// WithRetryDelay returns a copy of the status that tells the client to wait
// delay before it retries, as a google.rpc.RetryInfo detail. The detail
// replaces any RetryInfo the status had and leaves its other details alone. A
// negative delay is zero. Clients read the delay with RetryDelay, or the
// equivalent in their language.
func (s *Status) WithRetryDelay(delay time.Duration) *Status {
	if s == nil {
		s = OK()
	}

	details := append(withoutRetryInfo(s.Details), RetryInfoDetail(delay))
	return &Status{Code: s.Code, Message: s.Message, Metadata: cloneMetadata(s.Metadata), Details: cloneDetails(details)}
}

// RetryDelay returns how long the status tells the client to wait before it
// retries, from its first well-formed google.rpc.RetryInfo detail, and false
// when it has none. A detail that does not decode, or whose delay is negative
// or not a valid google.protobuf.Duration, is ignored. A delay too long for a
// time.Duration is the longest one.
func (s *Status) RetryDelay() (time.Duration, bool) {
	if s == nil {
		return 0, false
	}
	for _, detail := range s.Details {
		if detail.Type != retryInfoType {
			continue
		}
		if delay, ok := parseRetryInfo(detail.Value); ok {
			return delay, true
		}
	}
	return 0, false
}

// RetryDelay returns how long the *Status in err's chain tells the client to
// wait before it retries, as Status.RetryDelay does, such as the error of a
// call that a server's rate limit rejected. It reports false when err holds no
// status or its status carries no delay.
func RetryDelay(err error) (time.Duration, bool) {
	status, ok := errors.AsType[*Status](err)
	if !ok {
		return 0, false
	}
	return status.RetryDelay()
}

// appendRetryInfo appends the encoding of a RetryInfo whose retry_delay is
// delay to b. Like any protobuf encoder it leaves out a zero seconds or nanos.
func appendRetryInfo(b []byte, delay time.Duration) []byte {
	delay = max(delay, 0)
	seconds, nanos := int64(delay/time.Second), int64(delay%time.Second)
	var duration []byte
	if seconds != 0 {
		duration = protowire.AppendTag(duration, durationSecondsField, protowire.VarintType)
		duration = protowire.AppendVarint(duration, uint64(seconds))
	}
	if nanos != 0 {
		duration = protowire.AppendTag(duration, durationNanosField, protowire.VarintType)
		duration = protowire.AppendVarint(duration, uint64(nanos))
	}
	b = protowire.AppendTag(b, retryInfoDelayField, protowire.BytesType)
	return protowire.AppendBytes(b, duration)
}

// parseRetryInfo decodes a RetryInfo message. It reports false when the value
// is malformed, has no retry_delay, or holds a delay that is not a valid
// non-negative Duration: nanos must be below a second. Fields it does not know
// are skipped, and a repeated field takes its last value, as protobuf decoders
// do.
func parseRetryInfo(value []byte) (time.Duration, bool) {
	var duration []byte
	found := false
	for len(value) > 0 {
		number, kind, n := protowire.ConsumeTag(value)
		if n < 0 {
			return 0, false
		}
		value = value[n:]
		if number == retryInfoDelayField {
			if kind != protowire.BytesType {
				return 0, false
			}
			var m int
			duration, m = protowire.ConsumeBytes(value)
			if m < 0 {
				return 0, false
			}
			value, found = value[m:], true
			continue
		}
		if m := protowire.ConsumeFieldValue(number, kind, value); m >= 0 {
			value = value[m:]
		} else {
			return 0, false
		}
	}
	if !found {
		return 0, false
	}

	var seconds, nanos int64
	for len(duration) > 0 {
		number, kind, n := protowire.ConsumeTag(duration)
		if n < 0 {
			return 0, false
		}
		duration = duration[n:]
		if (number == durationSecondsField || number == durationNanosField) && kind == protowire.VarintType {
			v, m := protowire.ConsumeVarint(duration)
			if m < 0 {
				return 0, false
			}
			duration = duration[m:]
			if number == durationSecondsField {
				seconds = int64(v)
			} else {
				nanos = int64(int32(v))
			}
			continue
		}
		if number == durationSecondsField || number == durationNanosField {
			return 0, false
		}
		if m := protowire.ConsumeFieldValue(number, kind, duration); m >= 0 {
			duration = duration[m:]
		} else {
			return 0, false
		}
	}
	if seconds < 0 || nanos < 0 || nanos >= int64(time.Second) {
		return 0, false
	}
	if seconds > int64(math.MaxInt64/time.Second) {
		return math.MaxInt64, true
	}
	return time.Duration(seconds)*time.Second + time.Duration(nanos), true
}

// withoutRetryInfo returns a copy of details without the RetryInfo ones.
func withoutRetryInfo(details []Detail) []Detail {
	return slices.DeleteFunc(slices.Clone(details), func(detail Detail) bool { return detail.Type == retryInfoType })
}
