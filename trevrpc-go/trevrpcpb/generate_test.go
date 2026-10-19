package trevrpcpb_test

import (
	"bytes"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"testing"
)

const protoEnv = "TREVRPC_PROTO"

var update = flag.Bool("update", false, "rewrite options.pb.go from proto/trevrpc/options.proto")

// modernize is applied to generated code, so the committed file carries the
// rewrite that `modernize` would otherwise demand of protoc-gen-go's output.
func modernized(generated []byte) []byte {
	return bytes.ReplaceAll(generated, []byte("reflect.TypeOf(x{})"), []byte("reflect.TypeFor[x]()"))
}

// protocVersionLine is the only line of generated code that depends on the
// installed protoc binary; every other line is fixed by this module's versions.
var protocVersionLine = regexp.MustCompile(`(?m)^// \tprotoc +v.*\n`)

// TestGeneratedCodeMatchesProto regenerates options.pb.go from the shared
// proto/trevrpc/options.proto with protoc-gen-go and fails when the committed
// file has drifted. Run `go test ./trevrpcpb -update` to rewrite it.
//
// The proto directory (the include root that contains trevrpc/) defaults to
// the repository checkout and can be overridden with TREVRPC_PROTO.
func TestGeneratedCodeMatchesProto(t *testing.T) {
	protoDir := os.Getenv(protoEnv)
	if protoDir == "" {
		protoDir = filepath.Join("..", "..", "proto")
	}
	protoFile := filepath.Join(protoDir, "trevrpc", "options.proto")
	if _, err := os.Stat(protoFile); err != nil {
		t.Fatalf("cannot find %s (set %s to the proto directory): %v", protoFile, protoEnv, err)
	}
	protoc, err := exec.LookPath("protoc")
	if err != nil {
		t.Fatalf("protoc is required to verify the generated code: %v", err)
	}

	// Build the plugin from this module so its version matches go.mod.
	binDir := t.TempDir()
	run(t, "go", "build", "-o", binDir, "google.golang.org/protobuf/cmd/protoc-gen-go")

	outDir := t.TempDir()
	run(t, protoc,
		"--plugin=protoc-gen-go="+filepath.Join(binDir, "protoc-gen-go"),
		"-I", protoDir,
		"--go_out="+outDir, "--go_opt=paths=source_relative",
		protoFile,
	)

	const name = "options.pb.go"
	generated, err := os.ReadFile(filepath.Join(outDir, "trevrpc", name))
	if err != nil {
		t.Fatalf("protoc did not generate %s: %v", name, err)
	}
	generated = modernized(generated)
	if *update {
		if err := os.WriteFile(name, generated, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	committed, err := os.ReadFile(name)
	if err != nil {
		t.Fatalf("read committed %s: %v", name, err)
	}
	if !bytes.Equal(protocVersionLine.ReplaceAll(committed, nil), protocVersionLine.ReplaceAll(generated, nil)) {
		t.Errorf("%s is out of date with %s; run: go test ./trevrpcpb -update", name, protoFile)
	}
}

func run(t *testing.T, name string, args ...string) {
	t.Helper()
	command := exec.CommandContext(t.Context(), name, args...)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("%s %v: %v\n%s", name, args, err, output)
	}
}
