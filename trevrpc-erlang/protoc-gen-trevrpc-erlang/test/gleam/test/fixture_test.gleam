//// The code protoc-gen-trevrpc-gleam generates from proto/: every value
//// survives encoding and decoding, defaults encode to nothing, and what
//// Gleam cannot hold fails to decode. The values whose bytes protoc reads
//// back are written to out/.

import edge.{
  End, First, FirstChoiceValue, HTTPStatus, Holder, HolderKindBar, Keywords,
  ModeFast, ModeNone, ModeUnrecognized, Point3D, Second, SecondPickValue, Status,
  Stop, Tree,
}
import fixture/v1/common
import fixture/v1/enums
import fixture/v1/error.{Failure}
import fixture/v1/shapes.{
  ColorDarkBlue, ColorRed, ColorUnrecognized, Corner, Label, Radius, Shape,
  ShapeStyle, Size,
}
import gleam/dict
import gleam/option.{None, Some}
import gleeunit
import gleeunit/should

pub fn main() {
  gleeunit.main()
}

fn full_shape() -> shapes.Shape {
  Shape(
    name: "square",
    id: Some(-42),
    origin: Some(common.Point(1, -2)),
    points: [common.Point(0, 0), common.Point(3, 4)],
    color: ColorDarkBlue,
    blob: <<0, 255, 1>>,
    counts: dict.from_list([#("a", 1), #("b", -7)]),
    kind: Some(Corner(common.Point(9, 9))),
    style: Some(ShapeStyle(True, [ColorRed, ColorUnrecognized(77)])),
    tags: ["x", "üñ"],
    ratio: 0.5,
    big: 18_446_744_073_709_551_615,
    named: dict.from_list([#("p", common.Point(5, 6))]),
    colors: dict.from_list([#(1, ColorRed), #(2, ColorUnrecognized(9))]),
    accent: Some(ColorRed),
    weights: [1.5, -0.25],
    delta: -3,
    stamp: 1_700_000_000,
  )
}

fn tree() -> edge.Tree {
  Tree(
    name: "root",
    children: [
      Tree(name: "leaf", children: [], parent: None),
      Tree(..edge.new_tree(), name: "other", parent: Some(edge.new_tree())),
    ],
    parent: None,
  )
}

fn keywords() -> edge.Keywords {
  Keywords(
    type_: "t",
    fn_: 7,
    camel_case: "c",
    mode: ModeFast,
    maybe: Some(ModeNone),
    modes: dict.from_list([#("x", ModeFast), #("y", ModeUnrecognized(5))]),
  )
}

pub fn full_shape_test() {
  let bytes = shapes.encode_shape(full_shape())
  bytes |> shapes.decode_shape |> should.equal(Ok(full_shape()))
  write("out/fixture.v1.Shape.bin", bytes)
}

pub fn defaults_test() {
  shapes.new_shape() |> shapes.encode_shape |> should.equal(<<>>)
  <<>> |> shapes.decode_shape |> should.equal(Ok(shapes.new_shape()))
  <<>> |> edge.decode_keywords |> should.equal(Ok(edge.new_keywords()))
}

/// Set to the default is not unset where a field has presence.
pub fn presence_test() {
  let shape =
    Shape(
      ..shapes.new_shape(),
      id: Some(0),
      origin: Some(common.Point(0, 0)),
      accent: Some(shapes.ColorUnspecified),
    )
  shape |> shapes.encode_shape |> shapes.decode_shape |> should.equal(Ok(shape))
}

pub fn oneof_test() {
  [
    Radius(2.5),
    Label(""),
    Corner(common.Point(0, 0)),
    Size(common.Size(1.0, 2.0, common.UnitPoint)),
  ]
  |> list_each(fn(kind) {
    let shape = Shape(..shapes.new_shape(), kind: Some(kind))
    shape
    |> shapes.encode_shape
    |> shapes.decode_shape
    |> should.equal(Ok(shape))
  })
  let first = First(choice: Some(Stop(End)))
  first |> edge.encode_first |> edge.decode_first |> should.equal(Ok(first))
  let first = First(choice: Some(FirstChoiceValue(3)))
  first |> edge.encode_first |> edge.decode_first |> should.equal(Ok(first))
  let second = Second(pick: Some(Status(HTTPStatus(404))))
  second |> edge.encode_second |> edge.decode_second |> should.equal(Ok(second))
  let second = Second(pick: Some(SecondPickValue(-1)))
  second |> edge.encode_second |> edge.decode_second |> should.equal(Ok(second))
}

pub fn recursive_test() {
  let bytes = edge.encode_tree(tree())
  bytes |> edge.decode_tree |> should.equal(Ok(tree()))
  write("out/Tree.bin", bytes)
}

pub fn names_test() {
  let bytes = edge.encode_keywords(keywords())
  bytes |> edge.decode_keywords |> should.equal(Ok(keywords()))
  write("out/Keywords.bin", bytes)
  End |> edge.encode_end |> should.equal(<<>>)
  <<>> |> edge.decode_end |> should.equal(Ok(End))
  HTTPStatus(200)
  |> edge.encode_h_t_t_p_status
  |> edge.decode_h_t_t_p_status
  |> should.equal(Ok(HTTPStatus(200)))
  Point3D(1.5)
  |> edge.encode_point3_d
  |> edge.decode_point3_d
  |> should.equal(Ok(Point3D(1.5)))
}

/// A file named as Gleam's doc comments' words, imported by another, and a
/// file of enums only.
pub fn imported_test() {
  let failure =
    Failure(
      at: Some(common.Point(1, 1)),
      level: enums.LevelHigh,
      reason: error.ReasonLost,
      detail: "lost",
      tags: dict.from_list([#("k", "v")]),
    )
  let bytes = error.encode_failure(failure)
  bytes |> error.decode_failure |> should.equal(Ok(failure))
  write("out/fixture.v1.Failure.bin", bytes)
}

/// Without a package, a field and an enum value can share a message's name.
pub fn shared_names_test() {
  let holder = Holder(kind: HolderKindBar, point3_d: Some(Point3D(2.0)))
  let bytes = edge.encode_holder(holder)
  bytes |> edge.decode_holder |> should.equal(Ok(holder))
  write("out/Holder.bin", bytes)
  edge.Foo(3)
  |> edge.encode_foo
  |> edge.decode_foo
  |> should.equal(Ok(edge.Foo(3)))
  // The default kind is the value Foo, which gpb must not take for the
  // message.
  <<>> |> edge.decode_holder |> should.equal(Ok(edge.new_holder()))
}

/// A Gleam String is UTF-8, so a string that is not fails to decode, a map
/// key too.
pub fn invalid_utf8_test() {
  // name = <<255>>
  <<10, 1, 255>> |> shapes.decode_shape |> should.be_error
  // counts = {<<255>>: 1}
  <<58, 5, 10, 1, 255, 16, 1>> |> shapes.decode_shape |> should.be_error
}

pub fn malformed_test() {
  <<255, 255, 255>> |> shapes.decode_shape |> should.be_error
  // A length past the end.
  <<10, 5, 97>> |> edge.decode_tree |> should.be_error
}

/// Gleam floats are finite: a peer's infinity or NaN fails to decode.
pub fn unrepresentable_float_test() {
  // x = +infinity, a fixed64 field.
  <<9, 0, 0, 0, 0, 0, 0, 240, 127>> |> edge.decode_point3_d |> should.be_error
  // ratio = NaN, a fixed32 field.
  <<109, 0, 0, 192, 127>> |> shapes.decode_shape |> should.be_error
}

fn list_each(items: List(a), f: fn(a) -> b) -> Nil {
  case items {
    [] -> Nil
    [first, ..rest] -> {
      f(first)
      list_each(rest, f)
    }
  }
}

@external(erlang, "file", "write_file")
fn write(path: String, data: BitArray) -> a
