Comment for [DataDog/zstd#172](https://github.com/DataDog/zstd/issues/172).

+1, with another use case: per-message compression of small RPC messages (40–250 B) with a trained
dictionary. At those sizes the 4-byte magic number is 3–8% of each compressed frame, and the
copy-to-prepend workaround runs on every received message.

Since `ZSTD_c_format`/`ZSTD_d_format` are experimental parameters, a generic
`SetParameter`-style API on a reusable context (as discussed in #108) might cover this, checksums
(#43) and other frame options at once. That would need to be combined with dictionary support on
that context; see #174 for why `BulkProcessor` doesn't fit there today.
