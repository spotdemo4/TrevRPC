// Helpers for the well-known types google.protobuf.Timestamp, Duration, Struct, Value and
// ListValue, whose declarations generated code imports from the runtime.
import { int64ToBigInt } from "./int64.js";
import { invalidArgument } from "./status.js";

/** google.protobuf.NullValue. */
export const NullValue = Object.freeze({ NULL_VALUE: 0 });

const MsPerSecond = 1000n;
const NanosPerMs = 1_000_000;

/** Returns the Timestamp of a Date. */
export function timestampFromDate(date) {
  return timestampFromMs(date.getTime());
}

/** Returns the Timestamp of milliseconds since the Unix epoch. */
export function timestampFromMs(ms) {
  checkFinite(ms, "timestamp");
  const whole = Math.floor(ms);
  const seconds = Math.floor(whole / 1000);
  return { seconds: BigInt(seconds), nanos: (whole - seconds * 1000) * NanosPerMs };
}

/** Returns the Date of a Timestamp. */
export function timestampDate(timestamp) {
  return new Date(timestampMs(timestamp));
}

/** Returns the milliseconds since the Unix epoch of a Timestamp, rounded down. */
export function timestampMs(timestamp) {
  return toMs(timestamp, Math.floor);
}

/** Returns the Duration of a number of milliseconds. */
export function durationFromMs(ms) {
  checkFinite(ms, "duration");
  const whole = Math.trunc(ms);
  const seconds = Math.trunc(whole / 1000);
  // Seconds and nanos share the duration's sign; `+ 0` turns -0 into 0.
  return { seconds: BigInt(seconds), nanos: (whole - seconds * 1000) * NanosPerMs + 0 };
}

/** Returns the milliseconds of a Duration, rounded towards zero. */
export function durationMs(duration) {
  return toMs(duration, Math.trunc);
}

/** Returns the Struct of a JSON object. */
export function structFromJson(json) {
  if (json === null || typeof json !== "object" || Array.isArray(json)) {
    throw invalidArgument("a Struct is made from a JSON object");
  }
  const fields = {};
  for (const [key, value] of Object.entries(json)) {
    fields[key] = valueFromJson(value);
  }
  return { fields };
}

/** Returns the JSON object of a Struct. */
export function structToJson(struct) {
  const json = {};
  for (const [key, value] of Object.entries(struct?.fields ?? {})) {
    json[key] = valueToJson(value);
  }
  return json;
}

/** Returns the Value of a JSON value. */
export function valueFromJson(json) {
  if (json === null) {
    return { nullValue: NullValue.NULL_VALUE };
  }
  switch (typeof json) {
    case "number":
      return { numberValue: json };
    case "string":
      return { stringValue: json };
    case "boolean":
      return { boolValue: json };
    case "object":
      return Array.isArray(json)
        ? { listValue: { values: json.map(valueFromJson) } }
        : { structValue: structFromJson(json) };
    default:
      throw invalidArgument(`a Value cannot hold a ${typeof json}`);
  }
}

/** Returns the JSON value of a Value; a Value with no kind set is null. */
export function valueToJson(value) {
  switch (valueKind(value)) {
    case "numberValue":
      return value.numberValue;
    case "stringValue":
      return value.stringValue;
    case "boolValue":
      return value.boolValue;
    case "structValue":
      return structToJson(value.structValue);
    case "listValue":
      return (value.listValue.values ?? []).map(valueToJson);
    default:
      return null;
  }
}

const ValueKinds = [
  "nullValue",
  "numberValue",
  "stringValue",
  "boolValue",
  "structValue",
  "listValue",
];

/**
 * The member of a Value's `kind` oneof that is set. A decoded Value reads the defaults of the
 * other members from its prototype, so only own properties count.
 */
function valueKind(value) {
  return ValueKinds.find(
    (kind) => value != null && Object.hasOwn(value, kind) && value[kind] != null,
  );
}

/** The milliseconds of a Timestamp or Duration, with sub-millisecond nanos rounded by `round`. */
function toMs({ seconds, nanos } = {}, round) {
  return Number(int64ToBigInt(seconds) * MsPerSecond) + round((nanos ?? 0) / NanosPerMs);
}

function checkFinite(ms, what) {
  if (!Number.isFinite(ms)) {
    throw invalidArgument(`a ${what} needs a finite number of milliseconds, not ${ms}`);
  }
}
