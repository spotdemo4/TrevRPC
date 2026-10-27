package echopb_test

import (
	"bytes"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
)

const (
	goPackage = "trev.zip/llc/trevrpc/bench/baseline/echopb"
	protoEnv  = "TREVRPC_INTEROP_PROTO"
)

var update = flag.Bool("update", false, "rewrite the generated files from interop/proto/interop.proto")

// generatedFiles are relative to this package; protoc-gen-connect-go writes
// into an echopbconnect subpackage because its identifiers collide with
// protoc-gen-go-grpc's.
var generatedFiles = []string{"interop.pb.go", "interop_grpc.pb.go", "echopbconnect/interop.connect.go"}

// protocVersionLine is the only line of generated code that depends on the
// installed protoc binary; every other line is fixed by this module's versions.
var protocVersionLine = regexp.MustCompile(`(?m)^// (?:- )?\s*protoc +v.*\n`)

// TestGeneratedCodeMatchesProto regenerates the Echo bindings from the shared
// interop/proto/interop.proto with protoc-gen-go, protoc-gen-go-grpc, and
// protoc-gen-connect-go, and fails when the committed files have drifted. Run
// `go test ./echopb -update` to rewrite them.
//
// The proto directory defaults to the repository checkout and can be
// overridden with TREVRPC_INTEROP_PROTO.
func TestGeneratedCodeMatchesProto(t *testing.T) {
	protoDir := os.Getenv(protoEnv)
	if protoDir == "" {
		protoDir = filepath.Join("..", "..", "..", "interop", "proto")
	}
	protoFile := filepath.Join(protoDir, "interop.proto")
	if _, err := os.Stat(protoFile); err != nil {
		t.Fatalf("cannot find %s (set %s to the interop/proto directory): %v", protoFile, protoEnv, err)
	}
	protoc, err := exec.LookPath("protoc")
	if err != nil {
		t.Fatalf("protoc is required to verify the generated code: %v", err)
	}

	// Build the plugins from this module so their versions match go.mod.
	binDir := t.TempDir()
	run(t, "go", "build", "-o", binDir,
		"google.golang.org/protobuf/cmd/protoc-gen-go",
		"google.golang.org/grpc/cmd/protoc-gen-go-grpc",
		"connectrpc.com/connect/v2/cmd/protoc-gen-connect-go")

	outDir := t.TempDir()
	run(t, protoc,
		"--plugin=protoc-gen-go="+filepath.Join(binDir, "protoc-gen-go"),
		"--plugin=protoc-gen-go-grpc="+filepath.Join(binDir, "protoc-gen-go-grpc"),
		"--plugin=protoc-gen-connect-go="+filepath.Join(binDir, "protoc-gen-connect-go"),
		"-I", protoDir,
		"--go_out="+outDir, "--go_opt=paths=source_relative", "--go_opt=Minterop.proto="+goPackage,
		"--go-grpc_out="+outDir, "--go-grpc_opt=paths=source_relative", "--go-grpc_opt=Minterop.proto="+goPackage,
		"--connect-go_out="+outDir, "--connect-go_opt=paths=source_relative", "--connect-go_opt=Minterop.proto="+goPackage,
		protoFile,
	)

	for _, name := range generatedFiles {
		generated, err := os.ReadFile(filepath.Join(outDir, name))
		if err != nil {
			t.Fatalf("protoc did not generate %s: %v", name, err)
		}
		if *update {
			if err := os.MkdirAll(filepath.Dir(name), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(name, generated, 0o644); err != nil {
				t.Fatal(err)
			}
			continue
		}
		committed, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("read committed %s: %v", name, err)
		}
		if !bytes.Equal(protocVersionLine.ReplaceAll(committed, nil), protocVersionLine.ReplaceAll(generated, nil)) {
			t.Errorf("%s is out of date with %s; run: go test ./echopb -update", name, protoFile)
		}
	}
}

func run(t *testing.T, name string, args ...string) {
	t.Helper()
	command := exec.CommandContext(t.Context(), name, args...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, output)
	}
}
