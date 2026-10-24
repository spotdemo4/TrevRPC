package zstd

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"os"
	"strconv"
	"strings"
	"testing"
)

// maxMessage is the default max frame size, which too_large vectors exceed.
const maxMessage = 4 << 20

func loadVectors(t testing.TB) map[string][]byte {
	t.Helper()
	f, err := os.Open("../../../testdata/zstd-golden-vectors.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	vectors := map[string][]byte{}
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		line := sc.Text()
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			t.Fatalf("malformed line %q", line)
		}
		key, value = strings.TrimSpace(key), strings.TrimSpace(value)
		if strings.HasSuffix(key, ".id") {
			vectors[key] = []byte(value)
			continue
		}
		b, err := hex.DecodeString(value)
		if err != nil {
			t.Fatalf("%s: %v", key, err)
		}
		vectors[key] = b
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return vectors
}

func names(vectors map[string][]byte, prefix, suffix string) []string {
	var out []string
	for k := range vectors {
		if strings.HasPrefix(k, prefix) && strings.HasSuffix(k, suffix) {
			out = append(out, strings.TrimSuffix(k, suffix))
		}
	}
	return out
}

func TestDictionaryVector(t *testing.T) {
	v := loadVectors(t)
	id, err := DictID(v["dict.d1.dictionary"])
	if err != nil {
		t.Fatal(err)
	}
	want, _ := strconv.ParseUint(string(v["dict.d1.id"]), 16, 32)
	if uint64(id) != want {
		t.Fatalf("DictID = %d, want %d", id, want)
	}
	if fp := Fingerprint(v["dict.d1.dictionary"]); !bytes.Equal(fp[:], v["dict.d1.fingerprint"]) {
		t.Fatalf("Fingerprint = %x, want %x", fp, v["dict.d1.fingerprint"])
	}
	if _, err := DictID([]byte("raw content dictionary")); err == nil {
		t.Fatal("DictID accepted a raw-content dictionary")
	}
}

// decode is what a receiver holding only dict does with a payload.
func decode(dict *Dict, payload []byte, limit uint64) ([]byte, error) {
	h, err := ParseHeader(payload)
	if err != nil {
		return nil, err
	}
	if h.ContentSize > limit {
		return nil, errTooLarge
	}
	if h.DictID != dict.ID {
		return nil, ErrInvalid
	}
	return dict.Decompress(nil, payload, h)
}

var errTooLarge = errors.New("too large")

func TestGoldenVectors(t *testing.T) {
	if !Available {
		t.Skip("built without cgo")
	}
	v := loadVectors(t)
	dict, err := NewDict(v["dict.d1.dictionary"], 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range names(v, "ok.", ".payload") {
		got, err := decode(dict, v[name+".payload"], maxMessage)
		if err != nil {
			t.Errorf("%s: %v", name, err)
		} else if !bytes.Equal(got, v[name+".message"]) {
			t.Errorf("%s: decoded %x, want %x", name, got, v[name+".message"])
		}
	}
	for _, name := range names(v, "bad.", ".payload") {
		if got, err := decode(dict, v[name+".payload"], maxMessage); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: got %x, %v; want ErrInvalid", name, got, err)
		}
	}
	for _, name := range names(v, "too_large.", ".payload") {
		if _, err := decode(dict, v[name+".payload"], maxMessage); !errors.Is(err, errTooLarge) {
			t.Errorf("%s: got %v, want too large", name, err)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	if !Available {
		t.Skip("built without cgo")
	}
	v := loadVectors(t)
	dict, err := NewDict(v["dict.d1.dictionary"], 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range [][]byte{nil, v["ok.small.message"], v["ok.multi_segment.message"], v["ok.raw_block.message"]} {
		prefix := []byte("prefix")
		payload, err := dict.Compress(bytes.Clone(prefix), msg)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.HasPrefix(payload, prefix) {
			t.Fatal("Compress did not append")
		}
		payload = payload[len(prefix):]
		h, err := ParseHeader(payload)
		if err != nil {
			t.Fatalf("ParseHeader(%x): %v", payload, err)
		}
		if h.DictID != dict.ID || h.ContentSize != uint64(len(msg)) {
			t.Fatalf("header %+v for a %d-byte message", h, len(msg))
		}
		if payload[0]&0x04 != 0 {
			t.Fatal("payload has a checksum")
		}
		got, err := dict.Decompress([]byte("x"), payload, h)
		if err != nil || !bytes.Equal(got[1:], msg) {
			t.Fatalf("Decompress = %x, %v", got, err)
		}
	}
}

func FuzzParseHeader(f *testing.F) {
	v := loadVectors(f)
	for k, b := range v {
		if strings.HasSuffix(k, ".payload") {
			f.Add(b)
		}
	}
	f.Fuzz(func(t *testing.T, payload []byte) {
		h, err := ParseHeader(payload)
		if err == nil && h.DictID == 0 {
			t.Fatal("accepted Dictionary_ID 0")
		}
	})
}

func BenchmarkCompress(b *testing.B) {
	if !Available {
		b.Skip("built without cgo")
	}
	v := loadVectors(b)
	dict, err := NewDict(v["dict.d1.dictionary"], 1)
	if err != nil {
		b.Fatal(err)
	}
	msg := v["ok.small.message"]
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	for b.Loop() {
		buf, _ = dict.Compress(buf[:0], msg)
	}
}

func BenchmarkDecompress(b *testing.B) {
	if !Available {
		b.Skip("built without cgo")
	}
	v := loadVectors(b)
	dict, err := NewDict(v["dict.d1.dictionary"], 1)
	if err != nil {
		b.Fatal(err)
	}
	payload := v["ok.small.payload"]
	h, _ := ParseHeader(payload)
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	for b.Loop() {
		buf, _ = dict.Decompress(buf[:0], payload, h)
	}
}
