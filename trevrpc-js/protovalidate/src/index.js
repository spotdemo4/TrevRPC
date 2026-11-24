import { create, hasOption, toBinary } from "@bufbuild/protobuf";
import { RuntimeError, createValidator, violationsToProto } from "@bufbuild/protovalidate";
import {
  ViolationsSchema,
  field as fieldRules,
  message as messageRules,
  oneof as oneofRules,
} from "@bufbuild/protovalidate/gen/buf/validate/validate_pb.js";
import { Code, TrevRpcError } from "@trevrpc/trevrpc-js";

/**
 * Returns a request validator for the `validator` option of `createClient`, which enforces the
 * `buf.validate` rules of request messages with `validator`, a protovalidate validator, or with
 * one that `createValidator()` makes when it is undefined.
 */
export function createRequestValidator(validator = createValidator()) {
  // The request types warned about, so that each warns once however many methods take it.
  const warned = new Set();
  return {
    validateFunc(schema) {
      if (!hasRules(schema, new Set())) {
        return undefined;
      }
      // Compiles the rules now, through the validator's cache, by checking an empty message, so a
      // rule that cannot compile fails createClient instead of the first call. A rule that only
      // fails to evaluate for the empty message's default values, such as a division by a field
      // that is 0 by default, is left to each message's own check, which fails it with Internal.
      // protovalidate-es reports a rule naming a missing field that way too, and that rule fails
      // every request of the type, so it is logged as a warning.
      const compiled = validator.validate(schema, create(schema));
      if (compiled.kind === "error" && compiled.error instanceof RuntimeError) {
        if (!warned.has(schema.typeName)) {
          warned.add(schema.typeName);
          warnUnevaluated(schema.typeName, compiled.error);
        }
      } else if (compiled.kind === "error") {
        throw new TrevRpcError(
          Code.Internal,
          `cannot validate ${schema.typeName}: ${compiled.error.message}`,
          {},
          { cause: compiled.error },
        );
      }
      return (message) => {
        const result = validator.validate(schema, message);
        if (result.kind !== "valid") {
          throw statusFromResult(result);
        }
      };
    },
  };
}

/**
 * Warns that the rules of `typeName` cannot be evaluated for an empty message. A rule that fails
 * only for default values is fine, but protovalidate-es also reports a rule that names a field the
 * message lacks this way, which fails every request of the type: the warning makes that visible
 * when the client is created.
 */
function warnUnevaluated(typeName, error) {
  console.warn(
    `@trevrpc/protovalidate: the buf.validate rules of ${typeName} cannot be evaluated for an ` +
      `empty message (${error.message}). This is expected of a rule that fails for default ` +
      `values, such as a division by a field that is 0 by default, but a rule that names a field ` +
      `the message lacks fails every request of the type with Internal.`,
  );
}

/**
 * Converts a failed protovalidate result into the error a request fails with: InvalidArgument
 * with a `buf.validate.Violations` detail for broken rules, as TrevRPC servers report them, and
 * Internal for rules that could not be evaluated.
 */
function statusFromResult(result) {
  if (result.kind === "error") {
    return new TrevRpcError(
      Code.Internal,
      `request validation failed: ${result.error.message}`,
      {},
      { cause: result.error },
    );
  }
  const { violations } = result;
  if (violations.length === 0) {
    return new TrevRpcError(Code.InvalidArgument, "validation error", {}, { cause: result.error });
  }
  let message = `validation error: ${violations[0]}`;
  if (violations.length > 1) {
    message += ` (and ${violations.length - 1} more)`;
  }
  const [listed] = violationsToProto(violations);
  return new TrevRpcError(
    Code.InvalidArgument,
    message,
    {},
    {
      cause: result.error,
      details: [{ type: ViolationsSchema.typeName, value: toBinary(ViolationsSchema, listed) }],
    },
  );
}

/**
 * Reports whether `schema`, or a message type it holds however deep, carries `buf.validate`
 * rules. `seen` breaks recursive types.
 */
function hasRules(schema, seen) {
  if (seen.has(schema.typeName)) {
    return false;
  }
  seen.add(schema.typeName);
  if (hasOption(schema, messageRules)) {
    return true;
  }
  if (schema.oneofs.some((oneof) => hasOption(oneof, oneofRules))) {
    return true;
  }
  return schema.fields.some(
    (field) =>
      hasOption(field, fieldRules) ||
      (field.message !== undefined && hasRules(field.message, seen)),
  );
}
