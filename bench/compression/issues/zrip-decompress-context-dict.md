Posted as [paddor/zrip#94](https://github.com/paddor/zrip/issues/94).

# `DecompressContext::with_dict` fails on valid dictionary frames ("corrupt sequences section")

## Summary

A `DecompressContext` created with `DecompressContext::with_dict` rejects some valid
dictionary-compressed frames with `corrupt sequences section`. The one-shot
`decompress_with_dict_and_limit` decodes the same frames correctly with the same dictionary. The
failures don't depend on context reuse: a fresh context fails on the same frames.

The npm package shows the same bug: `Decompressor.withDict(d).decompress()` fails where
`decompressWithDict()` succeeds.

- zrip 0.8.13 (zrip-decode 0.8.11), and `@paddor/zrip` 0.5.14 on npm; also present on `main` at
  3d11557.
- The frames come from the reference libzstd 1.5.7 (via the `zstd` crate 0.13.3) and from zrip's
  own `Compressor::with_dict`. libzstd decodes all of them.

## Reproduction

`Cargo.toml`:

```toml
[package]
name = "zrip-dict-repro"
version = "0.0.0"
edition = "2024"

[dependencies]
zrip = "=0.8.13"
zstd = { version = "0.13", features = ["zdict_builder"] }
```

`src/main.rs`:

```rust
fn main() {
    // Deterministic, protobuf-encoded quote ticks: symbol, exchange, two doubles, two sizes,
    // timestamp. About 45 bytes each.
    let mut state = 0x2545_f491_4f6c_dd1du64;
    let mut rand = move |n: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % n
    };
    let symbols: Vec<String> = (0..500)
        .map(|_| (0..2 + rand(3)).map(|_| (b'A' + rand(26) as u8) as char).collect())
        .collect();
    let exchanges = ["XNAS", "XNYS", "ARCX", "BATS", "IEXG", "EDGX"];
    let mut i = 0u64;
    let mut record = || {
        i += 1;
        let varint = |out: &mut Vec<u8>, mut v: u64| {
            while v >= 0x80 {
                out.push(v as u8 | 0x80);
                v >>= 7;
            }
            out.push(v as u8);
        };
        let sym = &symbols[(symbols.len() as f64 * (rand(1000) as f64 / 1000.0).powi(3)) as usize];
        let bid = (500 + rand(49_500)) as f64 / 100.0;
        let mut m = vec![0x0a, sym.len() as u8];
        m.extend_from_slice(sym.as_bytes());
        m.extend_from_slice(&[0x12, 4]);
        m.extend_from_slice(exchanges[rand(6) as usize].as_bytes());
        m.push(0x19);
        m.extend_from_slice(&bid.to_le_bytes());
        m.push(0x21);
        m.extend_from_slice(&(bid + (1 + rand(20)) as f64 / 100.0).to_le_bytes());
        m.push(0x28);
        varint(&mut m, (1 + rand(50)) * 100);
        m.push(0x30);
        varint(&mut m, (1 + rand(50)) * 100);
        m.push(0x38);
        varint(&mut m, 1_790_000_000_000_000_000 + i * 1_000_000 + rand(1_000_000));
        m
    };
    let train: Vec<Vec<u8>> = (0..10_000).map(|_| record()).collect();
    let test: Vec<Vec<u8>> = (0..5_000).map(|_| record()).collect();
    let dict_bytes = zstd::dict::from_samples(&train, 8192).unwrap();

    let dict = zrip::Dictionary::from_bytes(&dict_bytes).unwrap();
    let mut ctx = zrip::DecompressContext::with_dict(dict.clone());
    let mut enc = zstd::bulk::Compressor::with_dictionary(1, &dict_bytes).unwrap();

    let (mut ctx_err, mut ctx_wrong, mut oneshot_bad) = (0, 0, 0);
    let mut first = None;
    for msg in &test {
        let frame = enc.compress(msg).unwrap();
        match ctx.decompress_with_limit(&frame, 1 << 20) {
            Ok(out) if out.as_ref() == msg.as_slice() => {}
            Ok(_) => ctx_wrong += 1,
            Err(e) => {
                ctx_err += 1;
                first.get_or_insert((e.to_string(), frame.len()));
            }
        }
        match zrip::decompress_with_dict_and_limit(&frame, &dict, 1 << 20) {
            Ok(out) if out == *msg => {}
            _ => oneshot_bad += 1,
        }
    }
    println!("DecompressContext::with_dict: {ctx_err} errors, {ctx_wrong} wrong output");
    println!("decompress_with_dict_and_limit: {oneshot_bad} failures");
    if let Some((e, len)) = first {
        println!("first error: {e} ({len} byte frame)");
    }
}
```

Output (x86_64 Linux, rustc 1.98.1, `--release`):

```text
DecompressContext::with_dict: 9 errors, 0 wrong output
decompress_with_dict_and_limit: 0 failures
first error: corrupt sequences section (53 byte frame)
```

The failure rate depends heavily on the data. On a larger corpus of small protobuf messages (six
message types, 39–231 bytes on average, dictionaries of 2–32 KiB trained with `zstd --train`, levels
1 and 3), 6,301 of 240,000 libzstd frames fail. With zrip's own `Compressor::with_dict` output, the
worst case is 71% of frames for one message type.

## Cause

`BlockDecodeWorkspace::cache_dict` (`crates/decode/src/lib.rs`) installs the dictionary's FSE
tables into `cached_dict_tables`, but leaves `ll_kind`, `of_kind` and `ml_kind` at their
`SequenceTableKind::Predefined` default from `SequenceDecodeTables::new_default()`.
`initial_sequence_state` then clones those tables into every frame.

`*_kind` records that a slot already holds the predefined table. When a block selects
Predefined_Mode for a sequence table, the decoder checks `prev.*_kind != Predefined`, finds
`Predefined`, and skips reloading the predefined table. It then decodes with the dictionary's
table instead.

The uncached path in `initial_sequence_state`, used by the one-shot functions, sets each `*_kind`
to `Other` when it installs a dictionary table, which is why those functions work.

## Fix

Set the kinds in `cache_dict` the same way:

```diff
--- a/crates/decode/src/lib.rs
+++ b/crates/decode/src/lib.rs
@@ -116,16 +116,19 @@ impl BlockDecodeWorkspace {
         if let Some((t, l)) = dict.of_table() {
             st.of_table = crate::seq_table::SeqTable::promote_of(t);
             st.of_accuracy = l;
+            st.of_kind = crate::sequences::SequenceTableKind::Other;
             st.of_set = true;
         }
         if let Some((t, l)) = dict.ml_table() {
             st.ml_table = crate::seq_table::SeqTable::promote_ml(t);
             st.ml_accuracy = l;
+            st.ml_kind = crate::sequences::SequenceTableKind::Other;
             st.ml_set = true;
         }
         if let Some((t, l)) = dict.ll_table() {
             st.ll_table = crate::seq_table::SeqTable::promote_ll(t);
             st.ll_accuracy = l;
+            st.ll_kind = crate::sequences::SequenceTableKind::Other;
             st.ll_set = true;
         }
         self.cached_dict_tables = Some(Box::new(st));
```

With this patch applied to `main` (3d11557):

- the reproduction above reports 0 errors;
- 0 of the 240,000 corpus frames fail;
- `cargo test -p zrip-decode` passes.

A regression test could decode a frame whose block uses Predefined_Mode for at least one sequence
table through `DecompressContext::with_dict`, comparing the result with
`decompress_with_dict_and_limit`.
