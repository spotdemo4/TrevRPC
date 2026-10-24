Posted as [klauspost/compress#1278](https://github.com/klauspost/compress/issues/1278).

# zstd: EncodeAll with a dictionary costs ~6–10 µs per small message, mostly building entropy tables

## Summary

Compressing small messages (40–250 bytes) one at a time with `EncodeAll` and a trained dictionary
costs about 5–11 µs per message at `SpeedFastest`. That's roughly 10× the cost of the same call
without a dictionary, and about 8× libzstd with the same dictionary. A smaller dictionary doesn't help: 2, 8 and
32 KiB dictionaries all cost 4.9–10 µs per message.

Profiles show most of the time goes to building and writing fresh Huffman and FSE tables for every
frame, plus resetting dictionary state. libzstd instead starts from the dictionary's entropy tables
for small inputs. For text-heavy messages the fresh tables also make frames larger than libzstd's.

This is the per-message RPC case: each message is its own frame, so it has to decode on its own,
and a trained dictionary is the only way small messages compress. #284 ("Faster small block
dictionary encoding", closed 2021) covered the state-reset part of this cost; the entropy-table
part remains.

Versions: v1.20.3, Go 1.26.8, linux/amd64.

## Reproduction

A self-contained test is below. It generates 20,000 training and 5,000 test protobuf-encoded
metric samples (236 bytes on average), trains an 8 KiB dictionary with `zstd --train`, and
benchmarks `EncodeAll` and `DecodeAll` with and without it. It needs the `zstd` CLI on `PATH`.

<details>
<summary><code>repro_test.go</code></summary>

```go
package repro

import (
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/klauspost/compress/zstd"
)

var (
	dir   string
	dict  []byte
	test  [][]byte
	names = []string{"http.server.request.duration", "http.server.active_requests", "rpc.server.duration",
		"db.client.operation.duration", "process.cpu.utilization", "runtime.go.gc.pause", "cache.hit_ratio"}
	services = []string{"checkout", "cart", "payments", "inventory", "search", "auth", "gateway"}
	regions  = []string{"us-east-1", "us-west-2", "eu-west-1", "eu-central-1", "ap-southeast-1"}
	routes   = []string{"/api/v1/orders", "/api/v1/orders/{id}", "/api/v1/cart", "/api/v1/users/{id}"}
)

type rng uint64

func (r *rng) n(k int) int {
	*r ^= *r << 13
	*r ^= *r >> 7
	*r ^= *r << 17
	return int(uint64(*r) % uint64(k))
}

func str(b []byte, field int, s string) []byte {
	b = binary.AppendUvarint(b, uint64(field<<3|2))
	b = binary.AppendUvarint(b, uint64(len(s)))
	return append(b, s...)
}

// metric encodes {name, map<string,string> labels, double value, int64 timestamp_ms}.
func metric(r *rng, i int, pods []string) []byte {
	b := str(nil, 1, names[r.n(len(names))])
	labels := [][2]string{
		{"service.name", services[r.n(len(services))]},
		{"deployment.environment", "production"},
		{"cloud.region", regions[r.n(len(regions))]},
		{"k8s.pod.name", pods[r.n(len(pods))]},
		{"http.route", routes[r.n(len(routes))]},
		{"http.response.status_code", []string{"200", "200", "200", "404", "500"}[r.n(5)]},
	}
	for _, kv := range labels {
		entry := str(str(nil, 1, kv[0]), 2, kv[1])
		b = binary.AppendUvarint(b, 2<<3|2)
		b = binary.AppendUvarint(b, uint64(len(entry)))
		b = append(b, entry...)
	}
	b = append(b, 3<<3|1)
	b = binary.LittleEndian.AppendUint64(b, math.Float64bits(float64(r.n(1_000_000))/1000))
	b = binary.AppendUvarint(b, 4<<3)
	return binary.AppendUvarint(b, uint64(1_790_000_000_000+i*10))
}

func TestMain(m *testing.M) {
	var err error
	if dir, err = os.MkdirTemp("", "zstd-dict-repro"); err != nil {
		panic(err)
	}
	defer os.RemoveAll(dir)
	r := rng(0x9e3779b97f4a7c15)
	pods := make([]string, 60)
	for i := range pods {
		pods[i] = fmt.Sprintf("%s-%08x-%04x", services[r.n(len(services))], r.n(1<<31), r.n(1<<16))
	}
	for _, split := range []struct {
		name string
		n    int
	}{{"train", 20000}, {"test", 5000}} {
		os.MkdirAll(filepath.Join(dir, split.name), 0o755)
		for i := range split.n {
			msg := metric(&r, i, pods)
			os.WriteFile(filepath.Join(dir, split.name, fmt.Sprint(i)), msg, 0o644)
			if split.name == "test" {
				test = append(test, msg)
			}
		}
	}
	out, err := exec.Command("zstd", "-q", "--train", "-r", filepath.Join(dir, "train"),
		"-o", filepath.Join(dir, "dict"), "--maxdict=8192").CombinedOutput()
	if err != nil {
		panic(fmt.Sprintf("zstd --train: %v: %s", err, out))
	}
	if dict, err = os.ReadFile(filepath.Join(dir, "dict")); err != nil {
		panic(err)
	}
	os.Exit(m.Run())
}

func encoder(tb testing.TB, withDict bool) *zstd.Encoder {
	opts := []zstd.EOption{zstd.WithEncoderLevel(zstd.SpeedFastest), zstd.WithEncoderCRC(false),
		zstd.WithEncoderConcurrency(1), zstd.WithSingleSegment(true)}
	if withDict {
		opts = append(opts, zstd.WithEncoderDict(dict))
	}
	enc, err := zstd.NewWriter(nil, opts...)
	if err != nil {
		tb.Fatal(err)
	}
	return enc
}

func decoder(tb testing.TB, withDict bool) *zstd.Decoder {
	opts := []zstd.DOption{zstd.WithDecoderConcurrency(1)}
	if withDict {
		opts = append(opts, zstd.WithDecoderDicts(dict))
	}
	dec, err := zstd.NewReader(nil, opts...)
	if err != nil {
		tb.Fatal(err)
	}
	return dec
}

func BenchmarkEncodeAll(b *testing.B) {
	for _, withDict := range []bool{false, true} {
		b.Run(fmt.Sprintf("dict=%v", withDict), func(b *testing.B) {
			enc := encoder(b, withDict)
			buf := make([]byte, 0, 1024)
			for i := 0; b.Loop(); i++ {
				buf = enc.EncodeAll(test[i%len(test)], buf[:0])
			}
		})
	}
}

func BenchmarkDecodeAll(b *testing.B) {
	for _, withDict := range []bool{false, true} {
		b.Run(fmt.Sprintf("dict=%v", withDict), func(b *testing.B) {
			enc, dec := encoder(b, withDict), decoder(b, withDict)
			frames := make([][]byte, len(test))
			for i, m := range test {
				frames[i] = enc.EncodeAll(m, nil)
			}
			buf := make([]byte, 0, 1024)
			var err error
			for i := 0; b.Loop(); i++ {
				if buf, err = dec.DecodeAll(frames[i%len(frames)], buf[:0]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
```

</details>

```text
$ go test -run '^$' -bench . -benchmem .
BenchmarkEncodeAll/dict=false-8     540.6 ns/op    0 B/op    0 allocs/op
BenchmarkEncodeAll/dict=true-8       6097 ns/op    0 B/op    0 allocs/op
BenchmarkDecodeAll/dict=false-8     147.8 ns/op    0 B/op    0 allocs/op
BenchmarkDecodeAll/dict=true-8      357.4 ns/op    0 B/op    0 allocs/op
```

For comparison, libzstd 1.5.7 at level 1 with the same dictionary, on the same messages cut into
independent 236-byte chunks: `zstd -b1 -D dict -B236 test.bin` reports 317.5 MB/s to compress
(≈0.74 µs per chunk) and 833 MB/s to decompress (≈0.28 µs per chunk). Frames are about the same
size for this message type: 57.5 bytes from `EncodeAll`, 56.1 bytes from `zstd -1 --no-check -D`.

## Profile

`BenchmarkEncodeAll/dict=true` (`-cpuprofile`, top entries):

```text
      flat  flat%   sum%        cum   cum%
     210ms 19.27% 19.27%      210ms 19.27%  runtime.memmove
     140ms 12.84% 32.11%      150ms 13.76%  huff0.(*Scratch).huffSort
     110ms 10.09% 42.20%      110ms 10.09%  zstd.(*fseEncoder).buildCTable
      70ms  6.42% 48.62%      240ms 22.02%  zstd.(*fastEncoderDict).Reset
      60ms  5.50% 54.13%      240ms 22.02%  huff0.(*Scratch).buildCTable
      50ms  4.59% 58.72%       80ms  7.34%  fse.(*cState).encodeZero (inline)
      50ms  4.59% 63.30%       50ms  4.59%  huff0.(*Scratch).countSimple
      50ms  4.59% 67.89%      210ms 19.27%  huff0.cTable.write
      40ms  3.67% 71.56%       40ms  3.67%  fse.(*Scratch).buildCTable
      30ms  2.75% 79.82%      140ms 12.84%  zstd.(*fseEncoder).normalizeCount
```

About 22% is `fastEncoderDict.Reset`, mostly `memmove`. Most of the rest is Huffman and FSE table
construction and serialization (`huff0.buildCTable`, `huffSort`, `cTable.write`,
`fseEncoder.buildCTable`, `normalizeCount`), even though the dictionary already supplies
literal-length, match-length, offset and Huffman tables that these blocks could reuse.

## Wider measurements

The same pattern holds across six message types, 39–231 bytes on average, with 32 KiB
`zstd --train` dictionaries at `SpeedFastest`:

| Message      | Mean size | Encode, ns (dict / no dict) | `EncodeAll` frame | libzstd L1 frame |
| ------------ | --------- | --------------------------- | ----------------- | ---------------- |
| chat text    | 180 B     | 9,470 / 332                 | 126.8 B           | 110.4 B          |
| metric       | 230 B     | 4,738 / 1,149               | 52.0 B            | 55.3 B           |
| log record   | 194 B     | 8,720 / –                   | 83.0 B            | 85.8 B           |
| order object | 231 B     | 10,848 / –                  | 141.7 B           | 125.2 B          |

Notes:

- "No dict" is from `BenchmarkVariants` with `SpeedFastest`; a dash means it wasn't measured for
  that type.
- libzstd frames here include a 4-byte checksum and the `EncodeAll` frames don't. Adjusted for
  that, libzstd is 14–16% smaller for the text and UUID-heavy chat and order messages, and about
  equal for the others.
- libzstd (native, `ZSTD_compress2` with a referenced CDict at level 1) took 670–1,070 ns per
  message on the same data.

## Possible direction

For small blocks with a dictionary, libzstd seeds the block's entropy state from the dictionary's
tables. It encodes literals and sequences in repeat mode when that is estimated to be cheaper, so
small frames carry no table descriptions at all. If the fast and dfast dictionary encoders
compared the cost of the dictionary's prebuilt tables against new ones, and skipped construction
when the dictionary's tables win, which is typical for inputs of a few hundred bytes, they could
avoid most of this work. Caching the serialized/prepared dictionary tables per encoder would make
that check cheap.

We'd happily test a branch against our corpus.
