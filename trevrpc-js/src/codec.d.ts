import type { DescMessage, Message, MessageShape } from "@bufbuild/protobuf";

/**
 * The generated codec of one message type, such as `GreetRequestCodec` from `greeter_trevrpc.js`,
 * which protoc-gen-trevrpc-js generates next to protoc-gen-es's `greeter_pb.js`. It encodes and
 * decodes the same messages, to the same bytes, as `toBinary` and `fromBinary` do with `schema`, in
 * code written for its type, and fails as they do, with their errors.
 *
 * Decoding differs on two kinds of malformed input: a known field with another wire type is kept
 * in `$unknown`, as the Go and Rust runtimes keep it, where `fromBinary` reads it as its declared
 * type anyway; and a nested message whose last field runs past its length makes the codec fall
 * back to `fromBinary`, which accepts it, so such input is decoded twice.
 *
 * Pass the codecs of a service, such as `GreeterCodecs`, as the `codecs` option of `createClient`,
 * or call `encode` and `decode` directly.
 */
export interface MessageCodec<Desc extends DescMessage = DescMessage> {
  /** The message type, the schema that protoc-gen-es generates. */
  readonly schema: Desc;
  /**
   * Encodes `message` to the bytes that `toBinary(schema, message)` produces. `message` must be a
   * message, made by `create()` or decoded, not an init object.
   */
  encode(message: MessageShape<Desc>): Uint8Array<ArrayBuffer>;
  /** Decodes a message as `fromBinary(schema, bytes)` does. */
  decode(bytes: Uint8Array): MessageShape<Desc>;
  /** Encodes a message for trevrpc-js's framing, which writes its frame header in front of it. */
  encodeBody?(message: MessageShape<Desc>): Uint8Array;
  /** Reads a message; for generated code only. */
  readonly read: (
    reader: Reader,
    end: number,
    depth: number,
    into: MessageShape<Desc> | undefined,
  ) => MessageShape<Desc>;
  /** Writes a message; for generated code only. */
  readonly write: (writer: Writer, message: MessageShape<Desc>) => void;
}

// What generated code uses. None of it is meant to be called directly.

export function defineCodec<Desc extends DescMessage>(
  schema: Desc,
  read: MessageCodec<Desc>["read"],
  write: MessageCodec<Desc>["write"],
): MessageCodec<Desc>;

export const RecursionLimit: 100;
export const EmptyBytes: Uint8Array<ArrayBuffer>;
export const Int64Zero: bigint | string;

export function readMessage<Desc extends DescMessage>(
  schema: Desc,
  reader: Reader,
  end: number,
  depth: number,
  into: MessageShape<Desc> | undefined,
): MessageShape<Desc>;
export function writeMessage<Desc extends DescMessage>(
  schema: Desc,
  writer: Writer,
  message: MessageShape<Desc>,
): void;
export function emptyMessage<Desc extends DescMessage>(schema: Desc): MessageShape<Desc>;
export function depthError(typeName: string): never;
export function foreignError(typeName: string, message: unknown): never;
export function setMapEntry(record: Record<string, unknown>, key: string, value: unknown): void;
export function intKey(key: string): number | string;
export function boolKey(key: string): boolean | string;
export function int64Key(key: string): bigint | string;
export function uint64Key(key: string): bigint | string;

type Long = bigint | string;

/** Reads the protobuf wire format; for generated code only. */
export class Reader {
  constructor(buf: Uint8Array);
  readonly buf: Uint8Array;
  readonly len: number;
  pos: number;
  tag(): number;
  uint32(): number;
  int32(): number;
  sint32(): number;
  bool(): boolean;
  double(): number;
  float(): number;
  fixed32(): number;
  sfixed32(): number;
  int64(): Long;
  uint64(): Long;
  sint64(): Long;
  fixed64(): Long;
  sfixed64(): Long;
  lenEnd(): number;
  string(): string;
  bytes(): Uint8Array;
  unknown(message: Message, tag: number, depth: number): void;
  skipTag(tag: number, depth: number): void;
  packedDouble(a: number[]): void;
  packedFloat(a: number[]): void;
  packedFixed32(a: number[]): void;
  packedSfixed32(a: number[]): void;
  packedUint32(a: number[]): void;
  packedInt32(a: number[]): void;
  packedSint32(a: number[]): void;
  packedBool(a: boolean[]): void;
  packedLong(a: unknown[], read: (this: Reader) => Long, asString: boolean): void;
  checkEnd(end: number): void;
}

/**
 * Writes the protobuf wire format; for generated code only. Field values are checked as
 * protobuf-es checks them, so the methods take any value.
 */
export class Writer {
  constructor(size?: number);
  finish(headroom?: boolean): Uint8Array<ArrayBuffer>;
  byte(b: number): void;
  varint(v: number): void;
  uint32(v: unknown): void;
  int32(v: unknown): void;
  sint32(v: unknown): void;
  bool(v: unknown): void;
  double(v: unknown): void;
  float(v: unknown): void;
  fixed32(v: unknown): void;
  sfixed32(v: unknown): void;
  int64(v: unknown): void;
  uint64(v: unknown): void;
  sint64(v: unknown): void;
  fixed64(v: unknown): void;
  sfixed64(v: unknown): void;
  bytes(v: Uint8Array): void;
  raw(v: Uint8Array): void;
  string(s: unknown): void;
  packedDouble(a: readonly number[]): void;
  packedFloat(a: readonly number[]): void;
  packedFixed32(a: readonly number[]): void;
  packedSfixed32(a: readonly number[]): void;
  packedUint32(a: readonly number[]): void;
  packedInt32(a: readonly number[]): void;
  packedSint32(a: readonly number[]): void;
  packedBool(a: readonly boolean[]): void;
  packedLong(a: readonly Long[], write: (this: Writer, v: Long) => void): void;
  fork(): number;
  join(at: number): void;
  unknown(fields: readonly { no: number; wireType: number; data: Uint8Array }[]): void;
}
