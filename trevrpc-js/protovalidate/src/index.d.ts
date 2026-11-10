import type { Validator } from "@bufbuild/protovalidate";
import type { RequestValidator } from "@trevrpc/trevrpc-js";

/**
 * Returns a request validator for the `validator` option of `createClient` in
 * `@trevrpc/trevrpc-js`, which enforces the `buf.validate` rules of request messages with
 * `validator`, or with one that protovalidate's `createValidator()` makes when it is undefined.
 * Pass a validator made with `createValidator({ registry })` to enforce predefined rules.
 *
 * A request message that breaks its rules is not sent: it fails with `Code.InvalidArgument` and a
 * `buf.validate.Violations` detail, which `error.findDetails(ViolationsSchema)` decodes, as TrevRPC
 * servers report requests that break them. One whose rules cannot be evaluated fails with
 * `Code.Internal`. Methods whose request type has no rules, directly or in a message it holds,
 * are not checked and cost nothing.
 *
 * `createClient` compiles the rules of each request type that has some, and throws a TrevRpcError
 * with `Code.Internal` when they cannot compile.
 */
export function createRequestValidator(validator?: Validator): RequestValidator;
