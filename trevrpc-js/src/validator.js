import { TrevRpcError, invalidArgument } from "./status.js";

/**
 * Returns the function that checks request messages of `schema` with `validator`, the
 * `validator` option of a client, or undefined when there is no validator or it never needs to
 * check that type, which costs the method nothing. The function throws a TrevRpcError: the one
 * the validator threw, or InvalidArgument with any other error as cause. Throws InvalidArgument
 * if `validator` is not a request validator, and what `validator.validateFunc` throws.
 */
export function requestValidation(validator, schema) {
  if (validator == null) {
    return undefined;
  }
  if (typeof validator.validateFunc !== "function") {
    throw invalidArgument("validator must have a validateFunc method");
  }
  const validate = validator.validateFunc(schema);
  if (validate == null) {
    return undefined;
  }
  if (typeof validate !== "function") {
    throw invalidArgument(
      `validator.validateFunc must return a function or nothing for ${schema.typeName}`,
    );
  }
  return (message) => {
    try {
      validate(message);
    } catch (error) {
      if (error instanceof TrevRpcError) {
        throw error;
      }
      throw invalidArgument(error?.message ?? String(error), error);
    }
  };
}
