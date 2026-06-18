package examplecert

import (
	"os"
	"path/filepath"
)

const envName = "TREVRPC_GO_EXAMPLE_CERT"

func Path() (string, error) {
	if path := os.Getenv(envName); path != "" {
		return path, nil
	}

	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}

	return filepath.Join(home, ".config", "trevrpc", "trevrpc-go-example-cert.pem"), nil
}
