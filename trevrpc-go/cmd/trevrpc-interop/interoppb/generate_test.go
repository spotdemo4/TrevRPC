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
	optionsEnv      = "TREVRPC_PROTO"
	generatorModule = "trev.zip/llc/trevrpc/trevrpc-go/cmd/protoc-gen-trevrpc-go"
)

var update = flag.Bool("update", false, "rewrite the generated files from interop/proto")

// protoFiles are the sources of the bindings, in interop/proto, and
// generatedFiles what they generate.
var (
	protoFiles     = []string{"interop.proto", "unreliable.proto"}
	generatedFiles = []string{"interop.pb.go", "interop.trevrpc.go", "unreliable.pb.go", "unreliable.trevrpc.go"}
)

// modernize is applied to generated code, so the committed files carry the
// rewrite that `modernize` would otherwise demand of protoc-gen-go's output.
func modernized(generated []byte) []byte {
	return bytes.ReplaceAll(generated, []byte("reflect.TypeOf(x{})"), []byte("reflect.TypeFor[x]()"))
}

// protocVersionLine is the only line of generated code that depends on the
// installed protoc binary; every other line is fixed by this module's versions.
var protocVersionLine = regexp.MustCompile(`(?m)^// \tprotoc +v.*\n`)

// TestGeneratedCodeMatchesProto regenerates the Echo and UnreliableEcho
// bindings from the shared interop/proto files with protoc-gen-go and this
// repository's protoc-gen-trevrpc-go, and fails when the committed files have
// drifted. Run `go test ./cmd/trevrpc-interop/interoppb -update` to rewrite
// them.
//
// The proto directories default to the repository checkout and can be
// overridden with TREVRPC_INTEROP_PROTO (interop/proto) and TREVRPC_PROTO
// (proto, which holds trevrpc/options.proto).
func TestGeneratedCodeMatchesProto(t *testing.T) {
	protoDir := os.Getenv(protoEnv)
	if protoDir == "" {
		protoDir = filepath.Join("..", "..", "..", "..", "interop", "proto")
	}
	optionsDir := os.Getenv(optionsEnv)
	if optionsDir == "" {
		optionsDir = filepath.Join("..", "..", "..", "..", "proto")
	}
	var sources []string
	for _, name := range protoFiles {
		source := filepath.Join(protoDir, name)
		if _, err := os.Stat(source); err != nil {
			t.Fatalf("cannot find %s (set %s to the interop/proto directory): %v", source, protoEnv, err)
		}
		sources = append(sources, source)
	}
	if _, err := os.Stat(filepath.Join(optionsDir, "trevrpc", "options.proto")); err != nil {
		t.Fatalf("cannot find trevrpc/options.proto in %s (set %s to the proto directory): %v", optionsDir, optionsEnv, err)
	}
	protoc, err := exec.LookPath("protoc")
	if err != nil {
		t.Fatalf("protoc is required to verify the generated code: %v", err)
	}

	// Build the plugins from this module so their versions match go.mod.
	binDir := t.TempDir()
	run(t, "", "go", "build", "-o", binDir, "google.golang.org/protobuf/cmd/protoc-gen-go", generatorModule)

	outDir := t.TempDir()
	args := []string{
		"--plugin=protoc-gen-go=" + filepath.Join(binDir, "protoc-gen-go"),
		"--plugin=protoc-gen-trevrpc-go=" + filepath.Join(binDir, "protoc-gen-trevrpc-go"),
		"-I", protoDir, "-I", optionsDir,
		"--go_out=" + outDir, "--go_opt=paths=source_relative",
		"--trevrpc-go_out=" + outDir, "--trevrpc-go_opt=paths=source_relative",
	}
	for _, name := range protoFiles {
		args = append(args, "--go_opt=M"+name+"="+goPackage, "--trevrpc-go_opt=M"+name+"="+goPackage)
	}
	run(t, "", protoc, append(args, sources...)...)

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
			t.Errorf("%s is out of date with %s; run: go test ./cmd/trevrpc-interop/interoppb -update", name, protoDir)
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
