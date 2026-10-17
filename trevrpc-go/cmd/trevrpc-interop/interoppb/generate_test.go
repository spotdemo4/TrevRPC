package interoppb_test

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
	goPackage       = "trev.zip/llc/trevrpc/trevrpc-go/cmd/trevrpc-interop/interoppb"
	protoEnv        = "TREVRPC_INTEROP_PROTO"
	generatorModule = "trev.zip/llc/trevrpc/trevrpc-go/cmd/protoc-gen-trevrpc-go"
)

var update = flag.Bool("update", false, "rewrite the generated files from interop/proto/interop.proto")

var generatedFiles = []string{"interop.pb.go", "interop.trevrpc.go"}

// modernize is applied to generated code, so the committed files carry the
// rewrite that `modernize` would otherwise demand of protoc-gen-go's output.
func modernized(generated []byte) []byte {
	return bytes.ReplaceAll(generated, []byte("reflect.TypeOf(x{})"), []byte("reflect.TypeFor[x]()"))
}

// protocVersionLine is the only line of generated code that depends on the
// installed protoc binary; every other line is fixed by this module's versions.
var protocVersionLine = regexp.MustCompile(`(?m)^// \tprotoc +v.*\n`)

// TestGeneratedCodeMatchesProto regenerates the Echo bindings from the shared
// interop/proto/interop.proto with protoc-gen-go and this repository's
// protoc-gen-trevrpc-go, and fails when the committed files have drifted. Run
// `go test ./cmd/trevrpc-interop/interoppb -update` to rewrite them.
//
// The proto directory defaults to the repository checkout and can be
// overridden with TREVRPC_INTEROP_PROTO.
func TestGeneratedCodeMatchesProto(t *testing.T) {
	protoDir := os.Getenv(protoEnv)
	if protoDir == "" {
		protoDir = filepath.Join("..", "..", "..", "..", "interop", "proto")
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
	run(t, "", "go", "build", "-o", binDir, "google.golang.org/protobuf/cmd/protoc-gen-go", generatorModule)

	outDir := t.TempDir()
	run(t, "", protoc,
		"--plugin=protoc-gen-go="+filepath.Join(binDir, "protoc-gen-go"),
		"--plugin=protoc-gen-trevrpc-go="+filepath.Join(binDir, "protoc-gen-trevrpc-go"),
		"-I", protoDir,
		"--go_out="+outDir, "--go_opt=paths=source_relative", "--go_opt=Minterop.proto="+goPackage,
		"--trevrpc-go_out="+outDir, "--trevrpc-go_opt=paths=source_relative", "--trevrpc-go_opt=Minterop.proto="+goPackage,
		protoFile,
	)

	for _, name := range generatedFiles {
		generated, err := os.ReadFile(filepath.Join(outDir, name))
		if err != nil {
			t.Fatalf("protoc did not generate %s: %v", name, err)
		}
		generated = modernized(generated)
		if *update {
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
			t.Errorf("%s is out of date with %s; run: go test ./cmd/trevrpc-interop/interoppb -update", name, protoFile)
		}
	}
}

func run(t *testing.T, dir, name string, args ...string) {
	t.Helper()
	command := exec.CommandContext(t.Context(), name, args...)
	command.Dir = dir
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, output)
	}
}
