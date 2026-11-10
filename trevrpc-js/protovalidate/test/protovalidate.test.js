import assert from "node:assert/strict";
import test from "node:test";

import { create } from "@bufbuild/protobuf";
import { createValidator } from "@bufbuild/protovalidate";
import { ViolationsSchema } from "@bufbuild/protovalidate/gen/buf/validate/validate_pb.js";
import { Code, TrevRpcError, createClient } from "@trevrpc/trevrpc-js";

// The in-memory server of trevrpc-js's own tests, which runs the real client end to end.
import { fakeTransport } from "../../test/support.js";
import {
  BatchRequestSchema,
  BrokenRequestSchema,
  ChoiceRequestSchema,
  CreateUserRequestSchema,
  FragileRequestSchema,
  LookupRequestSchema,
  NestedRequestSchema,
  RangeRequestSchema,
  TreeSchema,
  UserSchema,
  Users,
} from "../build/test/gen/rules_pb.js";
import { createRequestValidator } from "../src/index.js";

const valid = { name: "Ada", email: "ada@example.com" };

/** A Users client with request validation whose calls handler(serverStream) serves. */
function setup(handler, validator = createRequestValidator()) {
  const transport = fakeTransport(handler);
  return { transport, users: createClient(Users, transport, { validator }) };
}

/** Answers every call with the names of the request messages it receives, joined by commas. */
async function joinNames(stream) {
  await stream.readRequest();
  const names = [];
  for (let request; (request = await stream.readMessage(CreateUserRequestSchema)) !== null;) {
    names.push(request.name);
  }
  await stream.writeMessage(UserSchema, { name: names.join(",") });
  await stream.writeStatus(Code.Ok);
  await stream.finish();
}

/**
 * Checks that error is InvalidArgument whose Violations detail names exactly `fields`, the paths of
 * the fields that break their rules as dotted field names, such as "batch.users.name".
 */
function assertViolations(error, fields) {
  assert.ok(error instanceof TrevRpcError, `expected a TrevRpcError, got ${error}`);
  assert.equal(error.code, Code.InvalidArgument);
  assert.match(error.statusMessage, /^validation error: /);
  const [detail, ...others] = error.findDetails(ViolationsSchema);
  assert.equal(others.length, 0);
  assert.deepEqual(
    detail.violations.map((violation) =>
      (violation.field?.elements ?? []).map((element) => element.fieldName).join("."),
    ),
    fields,
  );
  return true;
}

test("requests that break their rules fail with their violations and are not sent", async () => {
  const { users, transport } = setup(() => assert.fail("no stream expected"));

  await assert.rejects(users.create({ name: "", email: "nope" }), (error) => {
    assertViolations(error, ["name", "email"]);
    assert.match(error.statusMessage, /\(and 1 more\)$/);
    return true;
  });
  await assert.rejects(users.watch({ name: "Ada" }), (error) => assertViolations(error, ["email"]));
  assert.equal(transport.opened, 0);
});

test("requests that keep their rules are sent", async () => {
  const { users, transport } = setup(joinNames);
  assert.equal((await users.create(valid)).name, "Ada");
  await transport.settled();
});

test("a streamed request message that breaks its rules fails its send only", async () => {
  const { users, transport } = setup(joinNames);
  const upload = await users.upload();
  await upload.send(valid);
  await assert.rejects(upload.send({ ...valid, name: "" }), (error) =>
    assertViolations(error, ["name"]),
  );
  await upload.send({ ...valid, name: "Grace" });
  assert.equal((await upload.closeAndRecv()).name, "Ada,Grace");
  await transport.settled();

  const chat = createClient(Users, fakeTransport(joinNames), {
    validator: createRequestValidator(),
  });
  const call = await chat.chat();
  await assert.rejects(call.send({ email: "x" }), (error) =>
    assertViolations(error, ["name", "email"]),
  );
  await call.close();
});

test("messages are found to have rules however deep they hold them", () => {
  const validator = createRequestValidator();
  for (const schema of [
    CreateUserRequestSchema,
    BatchRequestSchema,
    NestedRequestSchema,
    RangeRequestSchema,
    ChoiceRequestSchema,
  ]) {
    assert.equal(typeof validator.validateFunc(schema), "function", schema.typeName);
  }
  for (const schema of [LookupRequestSchema, TreeSchema, UserSchema]) {
    assert.equal(validator.validateFunc(schema), undefined, schema.typeName);
  }
});

test("rules held in nested messages, message rules and oneof rules are enforced", () => {
  const validator = createRequestValidator();
  // Clients hand the validator messages, not init objects.
  const check = (schema, init) => () => validator.validateFunc(schema)(create(schema, init));

  assert.throws(check(BatchRequestSchema, { users: { a: { ...valid, email: "x" } } }), (error) =>
    assertViolations(error, ["users.email"]),
  );
  check(BatchRequestSchema, { users: { a: valid } })();
  assert.throws(
    check(NestedRequestSchema, { batch: { users: [valid, { ...valid, name: "" }] } }),
    (error) => assertViolations(error, ["batch.users.name"]),
  );
  assert.throws(check(RangeRequestSchema, { start: 2, end: 1 }), (error) =>
    assertViolations(error, [""]),
  );
  check(RangeRequestSchema, { start: 1, end: 2 })();
  assert.throws(check(ChoiceRequestSchema, {}), (error) => assertViolations(error, ["kind"]));
  check(ChoiceRequestSchema, { kind: { case: "a", value: "x" } })();
});

test("rules that cannot compile fail createClient", () => {
  assert.throws(
    () => createRequestValidator().validateFunc(BrokenRequestSchema),
    (error) =>
      error instanceof TrevRpcError &&
      error.code === Code.Internal &&
      error.statusMessage.startsWith("cannot validate protovalidate.v1.BrokenRequest: ") &&
      error.cause !== undefined,
  );
});

test("rules that cannot be evaluated fail the request with Internal", () => {
  const check = createRequestValidator().validateFunc(FragileRequestSchema);
  const validate = (init) => check(create(FragileRequestSchema, init));
  validate({ number: "1" });
  assert.throws(
    () => validate({ number: "one" }),
    (error) =>
      error instanceof TrevRpcError &&
      error.code === Code.Internal &&
      error.statusMessage.startsWith("request validation failed: ") &&
      error.details.length === 0,
  );
  assert.throws(
    () => validate({ number: "-1" }),
    (error) => assertViolations(error, ["number"]),
  );
});

test("a protovalidate validator of the caller's is used", async () => {
  const { users } = setup(
    () => assert.fail("no stream expected"),
    createRequestValidator(createValidator({ failFast: true })),
  );
  await assert.rejects(users.create({ name: "", email: "nope" }), (error) => {
    assertViolations(error, ["name"]);
    assert.doesNotMatch(error.statusMessage, /more\)$/);
    return true;
  });
});
