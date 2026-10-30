package zstd

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
)

// corpus loads $TREVRPC_ZSTD_CORPUS: records.dict and test/<size>-<n>, the
// load benchmark's records payload and dictionary.
func corpus(b *testing.B) (dict []byte, bySize map[int][][]byte) {
	dir := os.Getenv("TREVRPC_ZSTD_CORPUS")
	if dir == "" {
		b.Skip("TREVRPC_ZSTD_CORPUS is unset")
	}
	dict, err := os.ReadFile(filepath.Join(dir, "records.dict"))
	if err != nil {
		b.Fatal(err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "test", "*"))
	sort.Strings(files)
	bySize = map[int][][]byte{}
	for _, f := range files {
		var size, n int
		_, _ = fmt.Sscanf(filepath.Base(f), "%d-%d", &size, &n)
		m, _ := os.ReadFile(f)
		bySize[size] = append(bySize[size], m)
	}
	return dict, bySize
}

var corpusSizes = []int{64, 256, 1024, 4096}

func corpusDicts(b *testing.B, data []byte) map[string]*Dict {
	d, err := NewDict(data, 1)
	if err != nil {
		b.Fatal(err)
	}
	return map[string]*Dict{"dict": d, "plain": NewPlain(1)}
}

func BenchmarkCorpusCompress(b *testing.B) {
	data, bySize := corpus(b)
	for _, kind := range []string{"dict", "plain"} {
		d := corpusDicts(b, data)[kind]
		for _, size := range corpusSizes {
			msgs := bySize[size]
			b.Run(fmt.Sprintf("%s/%d", kind, size), func(b *testing.B) {
				buf := make([]byte, 0, 2*size)
				var out int
				b.SetBytes(int64(size))
				i := 0
				for b.Loop() {
					buf, _ = d.Compress(buf[:0], msgs[i%len(msgs)])
					out += len(buf)
					i++
				}
				b.ReportMetric(float64(out)/float64(i), "B/frame")
			})
		}
	}
}

func BenchmarkCorpusDecompress(b *testing.B) {
	data, bySize := corpus(b)
	for _, kind := range []string{"dict", "plain"} {
		d := corpusDicts(b, data)[kind]
		for _, size := range corpusSizes {
			var payloads [][]byte
			var headers []Header
			for _, m := range bySize[size] {
				p, err := d.Compress(nil, m)
				if err != nil {
					b.Fatal(err)
				}
				h, err := ParseHeader(p)
				if err != nil {
					b.Fatal(err)
				}
				payloads, headers = append(payloads, p), append(headers, h)
			}
			b.Run(fmt.Sprintf("%s/%d", kind, size), func(b *testing.B) {
				buf := make([]byte, 0, size)
				b.SetBytes(int64(size))
				i := 0
				for b.Loop() {
					var err error
					if buf, err = d.Decompress(buf[:0], payloads[i%len(payloads)], headers[i%len(payloads)]); err != nil {
						b.Fatal(err)
					}
					i++
				}
			})
		}
	}
}

// BenchmarkCorpusStream sends one stream of a size's messages: each op
// encodes a message and decodes it on the other side.
func BenchmarkCorpusStream(b *testing.B) {
	data, bySize := corpus(b)
	for _, kind := range []string{"dict", "plain"} {
		d := corpusDicts(b, data)[kind]
		if kind == "plain" {
			d = nil
		}
		for _, size := range corpusSizes {
			msgs := bySize[size]
			b.Run(fmt.Sprintf("%s/%d", kind, size), func(b *testing.B) {
				enc, err := NewStreamEncoder(d, 1, StreamWindowLog)
				if err != nil {
					b.Fatal(err)
				}
				defer enc.Close()
				dec, err := NewStreamDecoder(StreamWindowLog)
				if err != nil {
					b.Fatal(err)
				}
				defer dec.Close()
				var chunk, msg []byte
				inFrame := false
				var out, i int
				b.SetBytes(int64(size))
				for b.Loop() {
					m := msgs[i%len(msgs)]
					if chunk, err = enc.Encode(chunk[:0], m); err != nil {
						b.Fatal(err)
					}
					info, err := ParseStreamChunk(chunk, !inFrame, StreamWindowLog)
					if err != nil {
						b.Fatal(err)
					}
					var dict *Dict
					if info.StartsFrame && info.DictID != 0 {
						dict = d
					}
					if msg, err = dec.Decode(msg[:0], chunk, info, dict, len(m)); err != nil {
						b.Fatal(err)
					}
					if len(chunk) > 0 {
						inFrame = !info.EndsFrame
					}
					out += len(chunk)
					i++
				}
				b.ReportMetric(float64(out)/float64(i), "B/chunk")
			})
		}
	}
}
