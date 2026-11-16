//// Request and response metadata: headers and trailers, by lower-case
//// name. Values are bytes; names ending in `-bin` hold binary values, the
//// others text.

import gleam/bit_array
import gleam/dict.{type Dict}
import gleam/list

/// Metadata names and their values.
pub type Metadata =
  Dict(String, BitArray)

/// No metadata.
pub fn new() -> Metadata {
  dict.new()
}

/// Metadata with text values.
pub fn from_list(entries: List(#(String, String))) -> Metadata {
  entries
  |> list.map(fn(entry) { #(entry.0, bit_array.from_string(entry.1)) })
  |> dict.from_list
}

/// A text value, if `name` has one that is UTF-8.
pub fn get(metadata: Metadata, name: String) -> Result(String, Nil) {
  case dict.get(metadata, name) {
    Ok(value) -> bit_array.to_string(value)
    Error(Nil) -> Error(Nil)
  }
}

/// Sets a text value.
pub fn insert(metadata: Metadata, name: String, value: String) -> Metadata {
  dict.insert(metadata, name, bit_array.from_string(value))
}
