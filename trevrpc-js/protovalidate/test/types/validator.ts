import { createValidator } from "@bufbuild/protovalidate";
import { createClient, type RequestValidator, type Transport } from "@trevrpc/trevrpc-js";

import { Users } from "../../build/test/gen/rules_pb.js";
import { createRequestValidator } from "../../src/index.js";

declare const transport: Transport;

createRequestValidator() satisfies RequestValidator;
createClient(Users, transport, { validator: createRequestValidator() });
createClient(Users, transport, {
  validator: createRequestValidator(createValidator({ failFast: true })),
});
// @ts-expect-error The validator is a protovalidate validator, not its options.
createRequestValidator({ failFast: true });
