//go:build !cgo || !linux || (!amd64 && !arm64)

package native

func Available() bool {
	return false
}
