// Package examplecert locates the TLS certificate the examples share.
package examplecert

import (
	"os"
	"path/filepath"
)

const envName = "TREVRPC_EXAMPLE_CERT"

// Path returns the example certificate path: $TREVRPC_EXAMPLE_CERT when set,
// otherwise a file under the user config directory.
func Path() (string, error) {
	if path := os.Getenv(envName); path != "" {
		return path, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(home, ".config", "trevrpc", "trevrpc-example-cert.pem"), nil
}
