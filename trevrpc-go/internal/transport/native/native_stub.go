//go:build !trevrpc_native || !cgo || (!linux && !darwin) || (!amd64 && !arm64)

package native

type Runtime struct{}

func Available() bool {
	return false
}

func CheckABI() (ABIInfo, error) {
	return ABIInfo{}, ErrUnavailable
}

func CheckTransportABI() (TransportABIInfo, error) {
	return TransportABIInfo{}, ErrUnavailable
}

func Open() (*Runtime, error) {
	return nil, ErrUnavailable
}

func (*Runtime) WakeSource() (WakeSource, error) {
	return WakeSource{}, ErrUnavailable
}

func (*Runtime) Close() error {
	return nil
}
