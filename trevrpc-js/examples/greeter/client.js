import { TrevRpcError, WebTransportClient } from "../../src/index.js";
import { GreeterClient } from "./greeter.trevrpc.js";

const form = document.querySelector("#client-form");
const output = document.querySelector("#output");
const submit = document.querySelector("#submit");

void loadExampleDefaults();

form.addEventListener("submit", (event) => {
  event.preventDefault();
  void runFromForm(new FormData(form));
});

for (const preset of document.querySelectorAll("[data-url]")) {
  preset.addEventListener("click", () => {
    form.elements.url.value = preset.dataset.url;
  });
}

async function runFromForm(data) {
  output.textContent = "";
  submit.disabled = true;

  const url = String(data.get("url") ?? "").trim();
  const name = String(data.get("name") ?? "TrevRPC").trim() || "TrevRPC";
  const token = String(data.get("token") ?? "").trim();
  const certificateHash = String(data.get("certificate-hash") ?? "").trim();

  let transport;
  try {
    log(`connecting to ${url}`);
    transport = await WebTransportClient.connect(url, {
      webTransportOptions: webTransportOptions(certificateHash),
    });
    const client = new GreeterClient(transport, {
      timeoutMs: 5_000,
      metadata: token === "" ? {} : { authorization: `Bearer ${token}` },
    });

    const unary = await client.sayHello({ name });
    log(`SayHello: ${unary.message}`);

    const replies = await client.lotsOfReplies({ name });
    for await (const reply of replies) {
      log(`LotsOfReplies: ${reply.message}`);
    }

    const summary = await client.lotsOfGreetings([
      { name: `${name} client stream 1` },
      { name: `${name} client stream 2` },
    ]);
    log(`LotsOfGreetings: ${summary.message}`);

    const bidiReplies = await client.bidiHello([
      { name: `${name} bidi 1` },
      { name: `${name} bidi 2` },
    ]);
    for await (const reply of bidiReplies) {
      log(`BidiHello: ${reply.message}`);
    }

    log("complete");
  } catch (error) {
    logError(error);
  } finally {
    submit.disabled = false;
    transport?.close({ closeCode: 0, reason: "example complete" });
  }
}

function webTransportOptions(certificateHash) {
  if (certificateHash === "") {
    return {};
  }

  return {
    serverCertificateHashes: [
      {
        algorithm: "sha-256",
        value: parseSha256Hash(certificateHash),
      },
    ],
  };
}

async function loadExampleDefaults() {
  try {
    const response = await fetch("./certificate-hash.json", { cache: "no-store" });
    if (!response.ok) {
      return;
    }

    const config = await response.json();
    if (typeof config.bearerToken === "string" && form.elements.token.value === "") {
      form.elements.token.value = config.bearerToken;
    }
    if (config.exists && typeof config.sha256Base64 === "string") {
      form.elements["certificate-hash"].value = config.sha256Base64;
      log(`loaded certificate hash from ${config.path}`);
    } else if (typeof config.path === "string") {
      log(`certificate not found at ${config.path}; start a Go or Rust example server first`);
    }
  } catch (error) {
    log(`could not load local certificate hash: ${error?.message ?? String(error)}`);
  }
}

function parseSha256Hash(value) {
  const normalized = value
    .trim()
    .replace(/^sha-?256[=:]/i, "")
    .trim();
  const hex = normalized.replace(/[:\s]/g, "");

  if (/^[0-9a-fA-F]{64}$/.test(hex)) {
    const bytes = new Uint8Array(32);
    for (let index = 0; index < bytes.length; index += 1) {
      bytes[index] = Number.parseInt(hex.slice(index * 2, index * 2 + 2), 16);
    }
    return bytes;
  }

  const base64 = normalized.replace(/-/g, "+").replace(/_/g, "/");
  const padded = base64.padEnd(base64.length + ((4 - (base64.length % 4)) % 4), "=");
  const binary = atob(padded);
  if (binary.length !== 32) {
    throw new Error("certificate hash must decode to 32 bytes of SHA-256 data");
  }

  const bytes = new Uint8Array(binary.length);
  for (let index = 0; index < binary.length; index += 1) {
    bytes[index] = binary.charCodeAt(index);
  }
  return bytes;
}

function log(message) {
  output.textContent += `${message}\n`;
}

function logError(error) {
  if (error instanceof TrevRpcError) {
    log(`RPC error ${error.code}: ${error.statusMessage || error.message}`);
    return;
  }

  log(`error: ${error?.message ?? String(error)}`);
}
