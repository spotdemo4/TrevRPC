package cabi_test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type sourceManifest struct {
	Schema              int                 `json:"schema"`
	CStandard           string              `json:"c_standard"`
	IncludePaths        []string            `json:"include_paths"`
	PlatformDefinitions map[string][]string `json:"platform_definitions"`
	SupportSources      []string            `json:"support_sources"`
	TranslationUnits    []translationUnit   `json:"translation_units"`
	Headers             []string            `json:"headers"`
}

type translationUnit struct {
	CMakeTarget string `json:"cmake_target"`
	Source      string `json:"source"`
	Wrapper     string `json:"wrapper"`
}

func TestSourceManifestMatchesCanonicalBuild(t *testing.T) {
	root := filepath.Clean(filepath.Join("..", ".."))
	contents, err := os.ReadFile(filepath.Join(root, "internal", "cabi", "sources.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest sourceManifest
	if err := json.Unmarshal(contents, &manifest); err != nil {
		t.Fatal(err)
	}
	if manifest.Schema != 1 || manifest.CStandard != "c11" {
		t.Fatalf("unexpected manifest schema or C standard: %+v", manifest)
	}
	cmake, err := os.ReadFile(filepath.Join(root, "CMakeLists.txt"))
	if err != nil {
		t.Fatal(err)
	}
	seenSources := map[string]bool{}
	seenWrappers := map[string]bool{}
	for _, unit := range manifest.TranslationUnits {
		if unit.CMakeTarget == "" || unit.Source == "" || unit.Wrapper == "" {
			t.Fatalf("incomplete translation unit: %+v", unit)
		}
		if seenSources[unit.Source] || seenWrappers[unit.Wrapper] {
			t.Fatalf("duplicate translation unit: %+v", unit)
		}
		seenSources[unit.Source] = true
		seenWrappers[unit.Wrapper] = true
		for _, path := range []string{unit.Source, unit.Wrapper} {
			if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(path))); err != nil {
				t.Fatalf("manifest path %q: %v", path, err)
			}
		}
		wrapper, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(unit.Wrapper)))
		if err != nil {
			t.Fatal(err)
		}
		relative, err := filepath.Rel(filepath.Dir(filepath.Join(root, filepath.FromSlash(unit.Wrapper))), filepath.Join(root, filepath.FromSlash(unit.Source)))
		if err != nil {
			t.Fatal(err)
		}
		expected := fmt.Sprintf("//go:build cgo && (linux || darwin)\n\n#include %q\n", filepath.ToSlash(relative))
		if string(wrapper) != expected {
			t.Fatalf("wrapper %q = %q, want %q", unit.Wrapper, wrapper, expected)
		}
		if !strings.Contains(string(cmake), unit.CMakeTarget) || !strings.Contains(string(cmake), unit.Source) {
			t.Fatalf("CMake does not contain target %q and source %q", unit.CMakeTarget, unit.Source)
		}
	}
	seenSupport := map[string]bool{}
	for _, source := range manifest.SupportSources {
		if seenSupport[source] || seenSources[source] {
			t.Fatalf("duplicate support source %q", source)
		}
		seenSupport[source] = true
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(source))); err != nil {
			t.Fatalf("manifest support source %q: %v", source, err)
		}
	}
	cgoSources, err := filepath.Glob(filepath.Join(root, "internal", "cabi", "*.c"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cgoSources) != len(manifest.TranslationUnits)+len(manifest.SupportSources) {
		t.Fatalf("manifest accounts for %d C sources, found %d: %v", len(manifest.TranslationUnits)+len(manifest.SupportSources), len(cgoSources), cgoSources)
	}
	seenHeaders := map[string]bool{}
	for _, header := range manifest.Headers {
		if seenHeaders[header] {
			t.Fatalf("duplicate header %q", header)
		}
		seenHeaders[header] = true
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(header))); err != nil {
			t.Fatalf("manifest header %q: %v", header, err)
		}
	}
	if len(manifest.IncludePaths) != 2 || manifest.PlatformDefinitions["linux"][0] != "TREVRPC_ENGINE_HAVE_PIPE2" {
		t.Fatalf("unexpected cgo build settings: %+v", manifest)
	}
}
