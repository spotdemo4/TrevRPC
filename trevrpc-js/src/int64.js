import protobuf from "protobufjs";

import { invalidArgument } from "./status.js";

/**
 * How decoded 64-bit integer fields are represented: `bigint`, `number` (exact up to 2^53),
 * decimal `string`, or protobuf.js's `long` (a Long object, as protobuf.js decodes them).
 */
export const Int64Representations = Object.freeze(["bigint", "number", "string", "long"]);

/** The representation of each root's 64-bit fields, for roots made with createRoot(). */
const rootRepresentations = new WeakMap();
/** The conversion plan of each message type, or null when its messages need none. */
const plans = new WeakMap();

const Int64Types = new Set(["int64", "uint64", "sint64", "fixed64", "sfixed64"]);
const UnsignedTypes = new Set(["uint64", "fixed64"]);

/**
 * Sets how the decoded messages of `root`'s types represent 64-bit integers. Without it they keep
 * protobuf.js's Long objects. A field's `jstype` option (`JS_STRING` or `JS_NUMBER`) overrides it.
 */
export function setInt64Representation(root, representation) {
  if (!Int64Representations.includes(representation)) {
    throw invalidArgument(
      `int64 must be one of ${Int64Representations.join(", ")}, not ${JSON.stringify(representation)}`,
    );
  }
  rootRepresentations.set(root, representation);
}

/**
 * Returns a function that converts, in place, the 64-bit integers of a message of `type` that
 * protobuf.js decoded, as its root's representation says, and returns the message; or undefined
 * when its messages need no conversion. 64-bit map keys, which protobuf.js decodes as 8-byte hash
 * strings, become decimal strings in every representation.
 */
export function int64Converter(type) {
  const plan = planFor(type);
  return plan === null ? undefined : (message) => applyPlan(plan, message);
}

/**
 * Converts a 64-bit integer, as a bigint, a number, a decimal string, or a Long-like object with
 * `low`, `high` and `unsigned`, to a bigint. Null and undefined are zero.
 */
export function int64ToBigInt(value, unsigned = value?.unsigned === true) {
  switch (typeof value) {
    case "bigint":
      return value;
    case "number":
      return BigInt(Math.trunc(value));
    case "string":
      return BigInt(value);
    default: {
      if (value == null) {
        return 0n;
      }
      const bits = (BigInt(value.high >>> 0) << 32n) | BigInt(value.low >>> 0);
      return unsigned ? bits : BigInt.asIntN(64, bits);
    }
  }
}

function planFor(type) {
  const cached = plans.get(type);
  if (cached !== undefined) {
    return cached;
  }
  const representation = rootRepresentations.get(type.root);
  if (representation === undefined) {
    plans.set(type, null);
    return null;
  }
  buildPlans(type, representation);
  return plans.get(type);
}

/**
 * Builds the plans of `start` and every message type reachable from it. A type needs a plan when
 * it has a 64-bit field to convert, or a message field whose type needs one; that is settled for
 * every reachable type at once, so recursive types are handled.
 */
function buildPlans(start, representation) {
  const steps = new Map();
  const pending = [start];
  steps.set(start, undefined);
  while (pending.length > 0) {
    const type = pending.pop();
    const typeSteps = [];
    for (const field of type.fieldsArray) {
      field.resolve();
      const step = fieldStep(type, field, representation);
      if (step !== undefined) {
        typeSteps.push(step);
      }
      if (field.resolvedType instanceof protobuf.Type && !steps.has(field.resolvedType)) {
        steps.set(field.resolvedType, undefined);
        pending.push(field.resolvedType);
      }
    }
    steps.set(type, typeSteps);
  }

  // A type needs a plan when it converts a value itself or holds a type that needs one.
  const needed = new Set();
  for (let changed = true; changed;) {
    changed = false;
    for (const [type, typeSteps] of steps) {
      if (
        !needed.has(type) &&
        typeSteps.some((step) => convertsItself(step) || needed.has(step.type))
      ) {
        needed.add(type);
        changed = true;
      }
    }
  }

  const built = new Map();
  for (const type of steps.keys()) {
    if (!plans.has(type)) {
      built.set(type, needed.has(type) ? [] : null);
      restoreNullDefaults(type);
    }
  }
  for (const [type, plan] of built) {
    plans.set(type, plan);
  }
  for (const [type, plan] of built) {
    if (plan === null) {
      continue;
    }
    for (const step of steps.get(type)) {
      const nested = step.type === undefined ? undefined : plans.get(step.type);
      if (!convertsItself(step) && nested == null) {
        continue;
      }
      plan.push({ ...step, nested: nested ?? undefined });
    }
    patchDefaults(type, plan);
  }
}

/** Reports whether a step converts values or keys of its own, not only nested messages. */
function convertsItself(step) {
  return step.convert !== undefined || step.convertKey !== undefined;
}

/**
 * The conversion step of one field of `type`, or undefined when it holds neither a 64-bit integer
 * nor a message. `convert` converts its 64-bit values, `convertKey` its 64-bit map keys, and
 * `type` is the message type of its values.
 */
function fieldStep(type, field, representation) {
  const messageType = field.resolvedType instanceof protobuf.Type ? field.resolvedType : undefined;
  const convert = Int64Types.has(field.type)
    ? valueConverter(fieldRepresentation(field, representation), UnsignedTypes.has(field.type))
    : undefined;
  if (field.map) {
    const convertKey = Int64Types.has(field.keyType)
      ? keyConverter(UnsignedTypes.has(field.keyType))
      : undefined;
    if (convert === undefined && convertKey === undefined && messageType === undefined) {
      return undefined;
    }
    return { name: field.name, shape: "map", convert, convertKey, type: messageType };
  }
  if (convert === undefined && messageType === undefined) {
    return undefined;
  }
  const shape = field.repeated ? "repeated" : "single";
  return { name: field.name, shape, convert, type: messageType };
}

/** The representation of a 64-bit field: its `jstype` option's, or the root's. */
function fieldRepresentation(field, representation) {
  switch (field.options?.jstype) {
    case "JS_STRING":
      return "string";
    case "JS_NUMBER":
      return "number";
    default:
      return representation;
  }
}

/** Returns a function converting a decoded 64-bit value, or undefined for Long objects. */
function valueConverter(representation, unsigned) {
  switch (representation) {
    case "bigint":
      return (value) => int64ToBigInt(value, unsigned);
    case "number":
      return (value) => Number(int64ToBigInt(value, unsigned));
    case "string":
      return (value) => int64ToBigInt(value, unsigned).toString();
    default:
      return undefined;
  }
}

/** Returns a function converting a decoded 64-bit map key, an 8-byte hash, to decimal. */
function keyConverter(unsigned) {
  return (key) => int64ToBigInt(protobuf.util.longFromHash(key, unsigned), unsigned).toString();
}

/**
 * Makes unset proto3 `optional` 64-bit fields of `type`'s messages read null, as other fields with
 * presence do: protobuf.js turns their null default into a Long zero.
 */
function restoreNullDefaults(type) {
  for (const field of type.fieldsArray) {
    if (Int64Types.has(field.type) && field.options?.proto3_optional && !field.repeated) {
      type.ctor.prototype[field.name] = null;
    }
  }
}

/**
 * Converts the defaults that `type`'s messages read from their prototype for unset singular 64-bit
 * fields, so that an unset field reads like a set one.
 */
function patchDefaults(type, plan) {
  const prototype = type.ctor.prototype;
  for (const step of plan) {
    // A field with presence, such as a proto3 optional one, reads null when unset.
    if (step.shape === "single" && step.convert !== undefined && prototype[step.name] != null) {
      prototype[step.name] = step.convert(prototype[step.name]);
    }
  }
}

function applyPlan(plan, message) {
  for (const step of plan) {
    if (!Object.hasOwn(message, step.name)) {
      continue;
    }
    const value = message[step.name];
    if (value == null) {
      continue;
    }
    switch (step.shape) {
      case "single":
        message[step.name] =
          step.convert === undefined ? applyPlan(step.nested, value) : step.convert(value);
        break;
      case "repeated":
        for (let index = 0; index < value.length; index += 1) {
          value[index] =
            step.convert === undefined
              ? applyPlan(step.nested, value[index])
              : step.convert(value[index]);
        }
        break;
      case "map":
        if (step.convertKey === undefined) {
          convertMapValues(step, value);
        } else {
          message[step.name] = convertMap(step, value);
        }
        break;
    }
  }
  return message;
}

/** Returns a copy of a decoded map with converted keys and values. */
function convertMap(step, map) {
  const converted = {};
  for (const [key, value] of Object.entries(map)) {
    converted[step.convertKey(key)] = convertMapValue(step, value);
  }
  return converted;
}

/** Converts the values of a decoded map in place. */
function convertMapValues(step, map) {
  for (const [key, value] of Object.entries(map)) {
    map[key] = convertMapValue(step, value);
  }
}

function convertMapValue(step, value) {
  if (step.convert !== undefined) {
    return step.convert(value);
  }
  return step.nested !== undefined && value != null ? applyPlan(step.nested, value) : value;
}
