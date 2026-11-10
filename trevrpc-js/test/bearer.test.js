import assert from "node:assert/strict";
import test from "node:test";

import { bearerMetadata } from "../src/index.js";
import { normalizeMetadata } from "../src/metadata.js";
import { Code, TrevRpcError } from "../src/status.js";

test("bearerMetadata carries a b64token as bearer credentials", () => {
  for (const token of ["abc", "abc.DEF-123_~+/", "abc.DEF-123_~+/==", "a=", "a"]) {
    assert.deepEqual(bearerMetadata(token), { authorization: `Bearer ${token}` });
  }
});

test("bearerMetadata rejects what is not a b64token", () => {
  for (const token of [
    "",
    "==",
    " token",
    "token ",
    "to ken",
    "token\n",
    "t\u00f6k\u00e9n",
    "a=b",
    "a,b",
    "\u0661",
    undefined,
    null,
    1,
  ]) {
    assert.throws(
      () => bearerMetadata(token),
      (error) => error instanceof TrevRpcError && error.code === Code.InvalidArgument,
      `${JSON.stringify(token)}`,
    );
  }
});

test("bearerMetadata is valid call and connection metadata", () => {
  const metadata = normalizeMetadata(bearerMetadata("abc=="));
  assert.equal(Buffer.from(metadata.authorization).toString(), "Bearer abc==");
});
