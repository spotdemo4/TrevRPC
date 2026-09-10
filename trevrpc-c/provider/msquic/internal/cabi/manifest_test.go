package cabi_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

type sourceManifest struct {
	Schema                int               `json:"schema"`
	CStandard             string            `json:"c_standard"`
	GoBuildConstraint     string            `json:"go_build_constraint"`
	IncludePaths          []string          `json:"include_paths"`
	Definitions           []string          `json:"definitions"`
	TranslationUnits      []translationUnit `json:"translation_units"`
	DeferredSources       []string          `json:"deferred_sources"`
	Headers               []string          `json:"headers"`
	ThirdPartyHeaders     []string          `json:"third_party_headers"`
	Archives              []archiveArtifact `json:"archives"`
	Licenses              []string          `json:"licenses"`
	Patch                 string            `json:"patch"`
	MirroredParentHeaders []string          `json:"mirrored_parent_headers"`
}

type translationUnit struct {
	CMakeTarget string `json:"cmake_target"`
	Source      string `json:"source"`
	Wrapper     string `json:"wrapper"`
}

type archiveArtifact struct {
	Target     string `json:"target"`
	Path       string `json:"path"`
	SHA256     string `json:"sha256"`
	Provenance string `json:"provenance"`
}

type provenance struct {
	Archive         string `json:"archive"`
	ArchiveSHA256   string `json:"archiveSHA256"`
	ProviderVersion string `json:"providerVersion"`
	Target          string `json:"target"`
	Version         string `json:"version"`
	Source          struct {
		Revision string `json:"rev"`
	} `json:"source"`
	Patch struct {
		SHA256 string `json:"sha256"`
	} `json:"patch"`
	Portability struct {
		TargetIndependentRuntimePaths bool `json:"targetIndependentRuntimePaths"`
	} `json:"portability"`
}

func loadManifest(t *testing.T) (string, sourceManifest) {
	t.Helper()
	root := filepath.Clean(filepath.Join("..", ".."))
	contents, err := os.ReadFile(filepath.Join(root, "internal", "cabi", "sources.json"))
	if err != nil {
		t.Fatal(err)
	}
	var manifest sourceManifest
	if err := json.Unmarshal(contents, &manifest); err != nil {
		t.Fatal(err)
	}
	return root, manifest
}

func TestSourceManifestMatchesCanonicalBuild(t *testing.T) {
	root, manifest := loadManifest(t)
	if manifest.Schema != 1 || manifest.CStandard != "c11" ||
		manifest.GoBuildConstraint != "cgo && linux && (amd64 || arm64)" {
		t.Fatalf("unexpected manifest build contract: %+v", manifest)
	}
	cmake, err := os.ReadFile(filepath.Join(root, "..", "..", "CMakeLists.txt"))
	if err != nil {
		t.Fatal(err)
	}
	seenSources := map[string]bool{}
	seenWrappers := map[string]bool{}
	for _, unit := range manifest.TranslationUnits {
		if unit.CMakeTarget == "" || unit.Source == "" || unit.Wrapper == "" ||
			seenSources[unit.Source] || seenWrappers[unit.Wrapper] {
			t.Fatalf("invalid or duplicate translation unit: %+v", unit)
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
		expected := fmt.Sprintf("//go:build %s\n\n#include %q\n", manifest.GoBuildConstraint, filepath.ToSlash(relative))
		if string(wrapper) != expected {
			t.Fatalf("wrapper %q = %q, want %q", unit.Wrapper, wrapper, expected)
		}
		cmakeSource := "${TREVRPC_MSQUIC_SOURCE_DIR}/" + filepath.Base(unit.Source)
		if !bytes.Contains(cmake, []byte(unit.CMakeTarget)) || !bytes.Contains(cmake, []byte(cmakeSource)) {
			t.Fatalf("CMake does not contain target %q and source %q", unit.CMakeTarget, cmakeSource)
		}
	}
	for _, source := range manifest.DeferredSources {
		if seenSources[source] {
			t.Fatalf("deferred source %q is also wrapped", source)
		}
		seenSources[source] = true
	}
	assertPathSet(t, root, "src", "*.c", seenSources)
	assertPathSet(t, root, "internal/cabi", "*.c", seenWrappers)
	assertPathSet(t, root, "include", "*.h", sliceSet(manifest.Headers, "include/"))
	assertPathSet(t, root, "src", "*.h", sliceSet(manifest.Headers, "src/"))
	assertPathSet(t, root, "third_party/msquic/include", "*.h", sliceSet(manifest.ThirdPartyHeaders, "third_party/msquic/include/"))
}

func TestMirroredParentHeadersMatch(t *testing.T) {
	root, manifest := loadManifest(t)
	for _, header := range manifest.MirroredParentHeaders {
		providerContents, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(header)))
		if err != nil {
			t.Fatal(err)
		}
		parentContents, err := os.ReadFile(filepath.Join(root, "..", "..", "include", filepath.Base(header)))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(providerContents, parentContents) {
			t.Errorf("mirrored header %q differs from parent", header)
		}
	}
}

func TestBundledArtifactsMatchProvenance(t *testing.T) {
	root, manifest := loadManifest(t)
	patchContents, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(manifest.Patch)))
	if err != nil {
		t.Fatal(err)
	}
	patchSum := sha256.Sum256(patchContents)
	expectedSums := make([]string, 0, len(manifest.Archives))
	for _, archive := range manifest.Archives {
		contents, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(archive.Path)))
		if err != nil {
			t.Fatalf("%s: %v", archive.Target, err)
		}
		sum := sha256.Sum256(contents)
		if got := hex.EncodeToString(sum[:]); got != archive.SHA256 {
			t.Errorf("%s SHA-256 = %s, want %s", archive.Target, got, archive.SHA256)
		}
		for _, forbidden := range [][]byte{[]byte("/nix/store/"), []byte("/build/")} {
			if bytes.Contains(contents, forbidden) {
				t.Errorf("%s archive contains forbidden path %q", archive.Target, forbidden)
			}
		}
		provenanceContents, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(archive.Provenance)))
		if err != nil {
			t.Fatal(err)
		}
		var record provenance
		if err := json.Unmarshal(provenanceContents, &record); err != nil {
			t.Fatal(err)
		}
		if record.Archive != "lib/libmsquic.a" || record.ArchiveSHA256 != archive.SHA256 ||
			record.ProviderVersion != "2.6.0-trevrpc.1" || record.Target != archive.Target ||
			record.Version != "2.6.0" || record.Source.Revision != "refs/tags/v2.6.0" ||
			!record.Portability.TargetIndependentRuntimePaths || record.Patch.SHA256 != hex.EncodeToString(patchSum[:]) {
			t.Errorf("%s provenance does not match bundled artifact contract: %+v", archive.Target, record)
		}
		expectedSums = append(expectedSums, fmt.Sprintf("%s  trevrpc-c/provider/msquic/%s", archive.SHA256, archive.Path))
	}
	for _, path := range manifest.Licenses {
		if info, err := os.Stat(filepath.Join(root, filepath.FromSlash(path))); err != nil || info.Size() == 0 {
			t.Errorf("license %q missing or empty: %v", path, err)
		}
	}
	sort.Strings(expectedSums)
	sums, err := os.ReadFile(filepath.Join(root, "provenance", "SHA256SUMS"))
	if err != nil {
		t.Fatal(err)
	}
	actualSums := strings.FieldsFunc(strings.TrimSpace(string(sums)), func(r rune) bool { return r == '\n' })
	sort.Strings(actualSums)
	if !reflect.DeepEqual(actualSums, expectedSums) {
		t.Errorf("SHA256SUMS = %q, want %q", actualSums, expectedSums)
	}
}

func sliceSet(paths []string, prefix string) map[string]bool {
	result := map[string]bool{}
	for _, path := range paths {
		if strings.HasPrefix(path, prefix) {
			result[path] = true
		}
	}
	return result
}

func assertPathSet(t *testing.T, root, directory, pattern string, expected map[string]bool) {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(root, filepath.FromSlash(directory), pattern))
	if err != nil {
		t.Fatal(err)
	}
	actual := map[string]bool{}
	for _, match := range matches {
		relative, err := filepath.Rel(root, match)
		if err != nil {
			t.Fatal(err)
		}
		actual[filepath.ToSlash(relative)] = true
	}
	if !reflect.DeepEqual(actual, expected) {
		t.Errorf("%s/%s paths = %v, want %v", directory, pattern, sortedKeys(actual), sortedKeys(expected))
	}
}

func sortedKeys(values map[string]bool) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
