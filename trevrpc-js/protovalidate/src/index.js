import { create, hasOption, toBinary } from "@bufbuild/protobuf";
import { createValidator, violationsToProto } from "@bufbuild/protovalidate";
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
  return {
    validateFunc(schema) {
      if (!hasRules(schema, new Set())) {
        return undefined;
      }
      // Compiles the rules now, through the validator's cache, so a rule that cannot compile fails
      // createClient instead of the first call.
      const compiled = validator.validate(schema, create(schema));
      if (compiled.kind === "error") {
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
