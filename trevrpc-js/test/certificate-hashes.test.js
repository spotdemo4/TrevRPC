import assert from "node:assert/strict";
import test from "node:test";

import { fetchServerCertificateHashes } from "../src/certificate-hashes.js";
import { Code } from "../src/status.js";

/** A fetch that answers every request with `body` as JSON and `status`, recording the requests. */
function fakeFetch(body, { status = 200 } = {}) {
  const requests = [];
  const fetch = async (url, init) => {
    requests.push({ url, init });
    return new Response(typeof body === "string" ? body : JSON.stringify(body), { status });
  };
  return { fetch, requests };
}

const hex = "00".repeat(31) + "ff";

test("fetchServerCertificateHashes parses the hex hashes a development server publishes", async () => {
  const { fetch, requests } = fakeFetch([
    { algorithm: "sha-256", value: hex },
    { algorithm: "sha-256", value: hex.toUpperCase() },
  ]);
  const hashes = await fetchServerCertificateHashes("/certificate-hashes", { fetch });
  const expected = new Uint8Array(32);
  expected[31] = 0xff;
  assert.deepEqual(hashes, [
    { algorithm: "sha-256", value: expected },
    { algorithm: "sha-256", value: expected },
  ]);
  assert.equal(requests[0].url, "/certificate-hashes");
  assert.equal(requests[0].init.cache, "no-store");
});

test("a 404 means a trusted certificate and yields no hashes", async () => {
  const { fetch } = fakeFetch("not found", { status: 404 });
  assert.deepEqual(await fetchServerCertificateHashes("/certificate-hashes", { fetch }), []);
});

test("other failures reject with Unavailable", async () => {
  const cases = [
    fakeFetch("oops", { status: 500 }).fetch,
    fakeFetch("not json").fetch,
    fakeFetch({ algorithm: "sha-256", value: hex }).fetch,
    fakeFetch([{ algorithm: "sha-256", value: "abc" }]).fetch,
    fakeFetch([{ value: hex }]).fetch,
    async () => {
      throw new TypeError("network down");
    },
  ];
  for (const fetch of cases) {
    await assert.rejects(fetchServerCertificateHashes("/h", { fetch }), (error) => {
      assert.equal(error.code, Code.Unavailable);
      return true;
    });
  }
});

test("an aborted fetch rejects with the abort reason", async () => {
  const controller = new AbortController();
  const reason = new Error("stop");
  controller.abort(reason);
  const fetch = async (_url, { signal }) => {
    throw signal.reason;
  };
  await assert.rejects(
    fetchServerCertificateHashes("/h", { fetch, signal: controller.signal }),
    (error) => error === reason,
  );
});
