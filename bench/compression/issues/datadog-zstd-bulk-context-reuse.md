Posted as [DataDog/zstd#174](https://github.com/DataDog/zstd/issues/174).

# BulkProcessor creates and frees a CCtx/DCtx on every call; no reusable context takes a dictionary

## Summary

`BulkProcessor` digests the dictionary once, which is what makes it the right API for many small
messages. But `Compress` and `Decompress` call `ZSTD_createCCtx`/`ZSTD_freeCCtx` and
`ZSTD_createDCtx`/`ZSTD_freeDCtx` on every call. For small messages that per-call setup is a large
share of the work. Keeping one context per caller cuts about 18% from compression and 46% from
decompression in the benchmark below.

The reusable `Ctx` from `NewCtx` doesn't accept a dictionary, so today there's no way to get both
digested dictionaries and context reuse through the package's API.

Versions: github.com/DataDog/zstd v1.5.7, Go 1.26.8, linux/amd64.

## Reproduction

`reuse.go` makes exactly the calls `BulkProcessor` makes (`ZSTD_compress_usingCDict`,
`ZSTD_decompress_usingDDict`) with the same digested dictionaries, but keeps one CCtx and one DCtx.
It links against the libzstd this package compiles.

<details>
<summary><code>reuse.go</code></summary>

```go
package repro

/*
#include <stddef.h>
size_t ZSTD_isError(size_t code);
void* ZSTD_createCCtx(void);
void* ZSTD_createDCtx(void);
void* ZSTD_createCDict(const void* dict, size_t size, int level);
void* ZSTD_createDDict(const void* dict, size_t size);
size_t ZSTD_compress_usingCDict(void* cctx, void* dst, size_t cap, const void* src, size_t len, const void* cdict);
size_t ZSTD_decompress_usingDDict(void* dctx, void* dst, size_t cap, const void* src, size_t len, const void* ddict);
*/
import "C"

import (
	"errors"
	"unsafe"

	_ "github.com/DataDog/zstd"
)

type reusedBulk struct{ cctx, dctx, cdict, ddict unsafe.Pointer }

func newReusedBulk(dict []byte, level int) *reusedBulk {
	d := unsafe.Pointer(&dict[0])
	return &reusedBulk{
		cctx:  C.ZSTD_createCCtx(),
		dctx:  C.ZSTD_createDCtx(),
		cdict: C.ZSTD_createCDict(d, C.size_t(len(dict)), C.int(level)),
		ddict: C.ZSTD_createDDict(d, C.size_t(len(dict))),
	}
}

func (r *reusedBulk) Compress(dst, src []byte) ([]byte, error) {
	n := C.ZSTD_compress_usingCDict(r.cctx, unsafe.Pointer(&dst[:1][0]), C.size_t(cap(dst)),
		unsafe.Pointer(&src[0]), C.size_t(len(src)), r.cdict)
	if C.ZSTD_isError(n) != 0 {
		return nil, errors.New("compress failed")
	}
	return dst[:n], nil
}

func (r *reusedBulk) Decompress(dst, src []byte) ([]byte, error) {
	n := C.ZSTD_decompress_usingDDict(r.dctx, unsafe.Pointer(&dst[:1][0]), C.size_t(cap(dst)),
		unsafe.Pointer(&src[0]), C.size_t(len(src)), r.ddict)
	if C.ZSTD_isError(n) != 0 {
		return nil, errors.New("decompress failed")
	}
	return dst[:n], nil
}
```

</details>

<details>
<summary><code>bulk_test.go</code></summary>

```go
package repro

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/DataDog/zstd"
)

// messages returns small JSON-ish records (~114 B), like RPC or log messages.
func messages(n int, seed uint64) [][]byte {
	services := []string{"checkout", "cart", "payments", "inventory", "search", "auth"}
	regions := []string{"us-east-1", "us-west-2", "eu-west-1", "ap-southeast-1"}
	rnd := func(k int) int {
		seed ^= seed << 13
		seed ^= seed >> 7
		seed ^= seed << 17
		return int(seed % uint64(k))
	}
	out := make([][]byte, n)
	for i := range out {
		out[i] = fmt.Appendf(nil, `{"service":%q,"region":%q,"status":%d,"latency_ms":%d,"user":%d,"trace":"%016x"}`,
			services[rnd(len(services))], regions[rnd(len(regions))], []int{200, 200, 404, 500}[rnd(4)],
			rnd(900), rnd(10_000_000), uint64(rnd(1<<62)))
	}
	return out
}

type bulk interface {
	Compress(dst, src []byte) ([]byte, error)
	Decompress(dst, src []byte) ([]byte, error)
}

func BenchmarkBulk(b *testing.B) {
	// A raw-content dictionary (concatenated samples) keeps the example self-contained; a trained
	// dictionary behaves the same way.
	dict := bytes.Join(messages(200, 1), nil)
	msgs := messages(5000, 2)
	bp, err := zstd.NewBulkProcessor(dict, 1)
	if err != nil {
		b.Fatal(err)
	}
	for _, c := range []struct {
		name string
		p    bulk
	}{{"BulkProcessor", bp}, {"reused-contexts", newReusedBulk(dict, 1)}} {
		frames := make([][]byte, len(msgs))
		for i, m := range msgs {
			f, err := c.p.Compress(make([]byte, 0, 1024), m)
			if err != nil {
				b.Fatal(err)
			}
			frames[i] = bytes.Clone(f)
		}
		b.Run(c.name+"/compress", func(b *testing.B) {
			buf := make([]byte, 0, 1024)
			for i := 0; b.Loop(); i++ {
				buf, _ = c.p.Compress(buf, msgs[i%len(msgs)])
			}
		})
		b.Run(c.name+"/decompress", func(b *testing.B) {
			buf := make([]byte, 0, 1024)
			for i := 0; b.Loop(); i++ {
				if buf, err = c.p.Decompress(buf, frames[i%len(frames)]); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
```

</details>

Medians of `go test -bench . -count 5`:

| Benchmark      | BulkProcessor | Reused contexts | Change |
| -------------- | ------------- | --------------- | ------ |
| Compress, ns   | 1437          | 1184            | −18%   |
| Decompress, ns | 414           | 224             | −46%   |

On a larger corpus of protobuf messages (180–231 B on average, trained 32 KiB dictionaries, level
1), the per-call context setup was 115–220 ns per compress and 190–225 ns per decompress.

`-benchmem` reports 0 allocations either way, because the per-call `ZSTD_createCCtx`/`createDCtx`
allocations happen on the C heap.

## Suggestions

1. **Pool contexts inside `BulkProcessor`** (e.g. a `sync.Pool` of CCtx/DCtx wrappers freed by
   finalizers, the way valyala/gozstd does). This keeps the API and the "safe for concurrent use"
   guarantee unchanged.
2. **Optionally, let a reusable context take a dictionary and parameters.** For example, a `Ctx`
   (or a separate compress/decompress context) with a referenced digested dictionary and
   `SetParameter`. That would also cover #172 (`ZSTD_f_zstd1_magicless`), #43 (checksums) and #108
   (context customization). Our use case is small RPC messages, one frame per message with a
   trained dictionary, where we want magicless frames as well.

Happy to send a PR for the first if it sounds acceptable.
