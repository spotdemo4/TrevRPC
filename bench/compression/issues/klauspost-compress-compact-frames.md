Posted as [klauspost/compress#1279](https://github.com/klauspost/compress/issues/1279).

# zstd: support magicless frames, and decoding frames without a dictionary ID

## Context

We compress small RPC messages (40–250 bytes) one frame per message with a trained dictionary.
At those sizes frame overhead is a real fraction of the output. libzstd lets a protocol drop parts
of the frame that it already knows or protects:

- the 4-byte magic number: `ZSTD_c_format` / `ZSTD_d_format` = `ZSTD_f_zstd1_magicless`;
- the content checksum: `ZSTD_c_checksumFlag` = 0;
- the dictionary ID: `ZSTD_c_dictIDFlag` = 0, when the dictionary is known out of band.

On our corpus, dropping all three takes a dictionary frame from about 15 bytes of overhead to
5 bytes, which is 5–20% of these messages. zstd-jni, the Rust `zstd` crate and libzstd itself all
expose these options. With klauspost/compress (v1.20.3), checksums can already be disabled
(`WithEncoderCRC(false)`), but the other two can't be used.

## 1. Magicless frames

There's no encoder or decoder option for the magicless format. Today we strip the 4 bytes after
`EncodeAll` and copy each received frame into a buffer behind a prepended magic number before
`DecodeAll`. That works but costs a copy per message.

Request: `WithEncoderMagicless(bool)` / `WithDecoderMagicless(bool)` (names to taste), matching
`ZSTD_f_zstd1_magicless`: the frame starts directly with the Frame_Header_Descriptor.

## 2. Frames without a dictionary ID

The decoder picks the dictionary by the frame's Dictionary_ID (`frame.o.dicts[frame.DictionaryID]`
in `decoder.go`). A frame that omits the ID looks up ID 0, but:

- `WithDecoderDicts` rejects a zstd-format dictionary whose header ID is 0 ("dictionaries cannot
  have ID 0");
- `WithDecoderDictRaw(0, content)` registers raw content only, without the dictionary's entropy
  tables.

So frames written by libzstd with `ZSTD_c_dictIDFlag = 0` and a trained dictionary can't be
decoded. In our test, 0 of 5,000 such frames decoded, apart from frames made only of raw blocks.

Request: a way to register a trained (zstd-format) dictionary as the one to use for frames
without a dictionary ID, e.g. `WithDecoderDefaultDict(dict []byte)`. Optionally, an encoder option
to omit the ID when encoding with `WithEncoderDict`.

For now we keep the dictionary ID in the frame, using IDs below 256 so it costs one byte. Item 1
is the one we'd use first.
