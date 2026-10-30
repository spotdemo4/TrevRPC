# zstd compression spike

The record of a spike that decided whether, and how, TrevRPC should compress messages. Messages
are mostly small and frequent, so it measured per-message zstd compression with trained
dictionaries across the reference libzstd (native, and compiled to wasm for browsers),
`@paddor/zrip`, Go's `klauspost/compress/zstd`, cgo libzstd wrappers, and the browser-native
`CompressionStream`. Only these results and the upstream issues it produced are kept; the
throwaway benchmark code is not.

## Method

- A seeded generator produced six synthetic protobuf message families (below), each with a
  20,000-message training split and a disjoint 5,000-message test split.
- The reference `zstd --train` trained per-family dictionaries of 2, 8 and 32 KiB and shared ones
  of 8, 32 and 110 KiB on the training splits.
- Every measurement compresses and decompresses each test message as its own frame, with contexts
  and digested dictionaries created once and reused, the way a runtime would. Every round trip was
  verified against the original message.
- The reference libzstd ran natively (through `zstd-sys`) and as a wasm module built from the same
  source (`zstd-sys` for `wasm32-unknown-unknown`, `wasm-ld`, `wasm-opt`), in Node 26, and in
  headless Chromium 156 and Firefox 157 through Playwright.
- Go measured klauspost/compress v1.20.3, libzstd's wasm build in wazero, DataDog/zstd v1.5.7,
  valyala/gozstd v1.26.0, and a cgo shim over libzstd's advanced API.
- All times are from one x86_64 Linux machine; compare them with each other, not across machines.

## Corpus

Low-cardinality values (metric names, label values, SKUs, symbols) come from pools shared by the
training and test splits, as within one deployment; IDs, timestamps, floats, trace IDs and text are
fresh per message, so dictionaries never see test values.

| Family     | Mean size | Shape                                                    |
| ---------- | --------- | -------------------------------------------------------- |
| `position` | 38.6 B    | entity ID, timestamp, six floats                         |
| `quote`    | 45.0 B    | symbol, exchange, two doubles, sizes, timestamp          |
| `chat`     | 180.3 B   | three UUIDs, name, short text                            |
| `metric`   | 230.2 B   | OpenTelemetry-style name and label map, value            |
| `log`      | 194.4 B   | templated body, attribute map, random trace and span IDs |
| `order`    | 231.2 B   | two UUIDs, 1-4 catalog line items, address               |

## Results

Times are per message on one core of the same machine; sizes are what the wire would carry (the
compressed frame, or the message itself when compression does not shrink it). "Lean" frames are
magicless, without checksum or dictionary ID. Dictionaries are per family, 32 KiB, level 1.

### Size

| Family   | Raw   | No dict | Dict, standard frame | Dict, lean frame | Dict, lean, level 3 | Shared 32 KiB dict, lean |
| -------- | ----- | ------- | -------------------- | ---------------- | ------------------- | ------------------------ |
| position | 38.6  | 38.6    | 38.6                 | 38.6             | 38.6                | 38.6                     |
| quote    | 45.0  | 45.0    | 44.7                 | 39.0             | 35.9                | 44.7                     |
| chat     | 180.3 | 164.9   | 110.4                | 100.4            | 96.2                | 116.8                    |
| metric   | 230.2 | 214.3   | 55.3                 | 45.3             | 43.4                | 50.6                     |
| log      | 194.4 | 194.4   | 85.8                 | 75.8             | 73.4                | 79.0                     |
| order    | 231.2 | 225.6   | 125.2                | 115.2            | 111.2               | 126.9                    |

- Without a dictionary, small protobuf messages do not compress (0-9 %), and the attempt costs
  3.5-3.8 µs each in native libzstd (5-5.6 µs in wasm), because every frame builds entropy tables
  from scratch.
- With a dictionary: metric ×5.1, log ×2.6, order ×2.0, chat ×1.8 (lean frames).
- `position` (floats) never compresses; `quote` only does with lean frames. Senders must fall back
  to an uncompressed MESSAGE per message.
- A standard frame spends ~15 B on overhead (magic 4, header 1, dictionary ID 2, content size 1,
  checksum 4, block header 3); a lean frame 5 B. On 40-230 B messages those 10 B are 5-20 %.
- Dictionary size barely matters (2 KiB gets most of the gain; 8 → 32 KiB is 2-6 %). Per-family
  dictionaries beat one shared dictionary by 4-14 %, and for `quote` the shared one is useless.
- Level 3 is 3-4 % smaller than level 1 (8 % for `quote`) for 5-45 % more compression time; level 6
  is up to 6.5× slower.

### Speed (dictionary, lean frame, level 1; compress / decompress, ns)

| Implementation                                        | chat          | metric        | log           | order         |
| ----------------------------------------------------- | ------------- | ------------- | ------------- | ------------- |
| libzstd native (Rust `zstd-sys`)                      | 917 / 283     | 638 / 179     | 745 / 236     | 1000 / 328    |
| libzstd wasm, Node 26                                 | 1723 / 905    | 957 / 709     | 1423 / 847    | 1841 / 1080   |
| libzstd wasm, Chromium 156                            | 1765 / 970    | 975 / 786     | 1418 / 919    | 1891 / 1150   |
| libzstd wasm, Firefox 157                             | 1835 / 1107   | 1141 / 932    | 1310 / 904    | 1972 / 1245   |
| zrip, Chromium 156 (std frames)                       | 6174 / 5638   | 4770 / 3730   | 6408 / 3717   | 6777 / 6050   |
| klauspost, Go (std frames, no CRC)                    | 9470 / 2117   | 4738 / 389    | 8720 / 360    | 10848 / 2362  |
| libzstd wasm in wazero, Go                            | 2944 / 946    | 2001 / 836    |               |               |
| `CompressionStream("deflate-raw")`, Chromium, no dict | 34109 / 25368 | 44259 / 25979 | 38663 / 27567 | 41852 / 24900 |

Size of the same messages with zrip: chat 132.7, metric 58.4, log 91.0, order 148.4; with klauspost
(fastest, standard frame minus magic): 122.8, 48.0, 79.0, 137.7. Both build fresh entropy tables
per message instead of reusing the dictionary's, which is slower and writes table descriptions into
every frame. zrip at level 3 takes ~100 µs per message.

### Wasm module size

| Build                                         | Raw    | gzip   | brotli |
| --------------------------------------------- | ------ | ------ | ------ |
| libzstd, all levels                           | 500 KB | 130 KB |        |
| libzstd, fast + dfast strategies (levels 1-4) | 266 KB | 85 KB  | 68 KB  |
| libzstd, fast strategy only (level 1)         | 227 KB | 79 KB  | 64 KB  |
| libzstd decoder only                          | 111 KB | 40 KB  |        |
| libzstd decoder only, zstd-sys `thin`         | 80 KB  | 31 KB  | 24 KB  |
| zrip (one of two shipped builds)              | 371 KB | 92 KB  |        |

SIMD builds are no faster (libzstd has no wasm SIMD paths). Instantiation takes 0.3-2.5 ms in
Chromium and up to 17 ms in Firefox; digesting a 32 KiB dictionary for both directions takes
about 0.2 ms (at most 2.1 ms).

### Correctness and interop

- zrip 0.5.14: `Decompressor.withDict(d).decompress()` fails on up to 71 % of valid dictionary
  frames (from zrip or libzstd, depending on family) with "corrupt sequences section", even on a
  fresh instance; the one-shot `decompressWithDict()` decodes all of them. The benchmark uses the
  one-shot function, whose decode times above are therefore an upper bound for zrip.
- klauspost writes multi-segment frames below 1 KiB, and those below 256 B omit the content size;
  `WithSingleSegment(true)` fixes both. It has no magicless option (strip and re-add the 4 bytes),
  and it selects the decoding dictionary by the frame's dictionary ID and refuses ID 0, so frames
  without a dictionary ID cannot be decoded with a trained dictionary.
- With those accommodations, klauspost ↔ libzstd and zrip ↔ libzstd decode every frame both ways.
- `CompressionStream`: Chromium 156 has neither `zstd` nor `brotli`; Firefox 157 has `brotli` only.
  It is async-only and takes no dictionary.
- zstd-jni (Kotlin) exposes `setMagicless`, `setDictID`, `setChecksum` on both contexts.

### Go cgo wrappers

No Go cgo wrapper exposes libzstd's frame parameters: DataDog/zstd sets only the level and worker
count, valyala/gozstd only the window log. Both dictionary APIs write libzstd's default dictionary
frame (magic number, dictionary ID, content size, no checksum). The shim is ~60 lines of cgo over
the advanced API (reused contexts, `ZSTD_c_format` magicless) that vendors no C: it declares the
prototypes it calls and blank-imports DataDog/zstd for the compiled libzstd objects. TrevRPC's Go
runtime uses this approach until klauspost/compress handles small dictionary messages well; binaries
built without cgo use klauspost/compress, stripping and restoring the magic number.

Per message, family 32 KiB dictionaries, level 1, encode / decode ns and frame size:

| Wrapper                                    | chat              | metric          | log              | order             |
| ------------------------------------------ | ----------------- | --------------- | ---------------- | ----------------- |
| DataDog `BulkProcessor` (context per call) | 1320 / 590, 106 B | 980 / 510, 51 B | 1070 / 525, 82 B | 1460 / 670, 121 B |
| valyala `CompressDict` (pooled contexts)   | 1020 / 360, 106 B | 700 / 245, 51 B | 800 / 285, 82 B  | 1130 / 410, 121 B |
| shim, standard frame                       | 1160 / 400, 106 B | 840 / 285, 51 B | 955 / 325, 82 B  | 1240 / 465, 121 B |
| shim, magicless                            | 1135 / 395, 102 B | 855 / 280, 47 B | 930 / 320, 78 B  | 1300 / 455, 117 B |
| klauspost (for reference)                  | 9470 / 2117       | 4738 / 389      | 8720 / 360       | 10848 / 2362      |

valyala links prebuilt `.a` files committed to its repository, for a fixed platform list. DataDog
compiles its bundled libzstd 1.5.7 with cgo's default `-O2` (plus legacy-format and multithreading
support), which accounts for the shim being ~15 % slower than valyala.

### Upstream issues

| Issue                                                                        | Report                                                                                                                                   |
| ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- |
| [paddor/zrip#94](https://github.com/paddor/zrip/issues/94)                   | [`DecompressContext::with_dict` rejects valid frames](issues/zrip-decompress-context-dict.md) (root cause and a verified three-line fix) |
| [klauspost/compress#1278](https://github.com/klauspost/compress/issues/1278) | [Per-message dictionary encode cost](issues/klauspost-compress-small-dict-encode.md)                                                     |
| [klauspost/compress#1279](https://github.com/klauspost/compress/issues/1279) | [Magicless frames and frames without a dictionary ID](issues/klauspost-compress-compact-frames.md)                                       |
| [DataDog/zstd#174](https://github.com/DataDog/zstd/issues/174)               | [`BulkProcessor` creates a context per call](issues/datadog-zstd-bulk-context-reuse.md)                                                  |
| [DataDog/zstd#172](https://github.com/DataDog/zstd/issues/172)               | [Comment: our use case for magicless frames](issues/datadog-zstd-172-comment.md)                                                         |
