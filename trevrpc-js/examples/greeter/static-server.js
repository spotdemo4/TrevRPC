import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { readFile, stat } from "node:fs/promises";
import { createServer } from "node:http";
import { homedir } from "node:os";
import { extname, join, normalize, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const port = Number.parseInt(process.env.PORT ?? "8080", 10);
const authToken = "trevrpc-example-token";
const protobufBundlePath = fileURLToPath(import.meta.resolve("protobufjs/dist/protobuf.min.js"));

const contentTypes = new Map([
  [".html", "text/html; charset=utf-8"],
  [".js", "text/javascript; charset=utf-8"],
  [".json", "application/json; charset=utf-8"],
  [".md", "text/markdown; charset=utf-8"],
  [".proto", "text/plain; charset=utf-8"],
]);

const server = createServer(async (request, response) => {
  try {
    const path = requestPath(request.url ?? "/");

    if (path === "/favicon.ico") {
      response.writeHead(204);
      response.end();
      return;
    }

    if (path === "/examples/greeter/certificate-hash.json") {
      await writeCertificateHash(response);
      return;
    }

    if (path === "/examples/greeter/vendor/protobufjs.js") {
      writeProtobufModule(response);
      return;
    }

    if (path === "/examples/greeter/vendor/protobuf.min.js") {
      await writeProtobufBundle(response);
      return;
    }

    const file = path.endsWith("/") ? join(path, "index.html") : path;
    await writeStaticFile(response, safePath(file));
  } catch (error) {
    if (error?.code === "ENOENT") {
      notFound(response);
      return;
    }

    response.writeHead(500, { "content-type": "text/plain; charset=utf-8" });
    response.end(error?.message ?? String(error));
  }
});

server.listen(port, "127.0.0.1", () => {
  console.log(`serving trevrpc-js from http://127.0.0.1:${port}/examples/greeter/`);
});

function requestPath(url) {
  return decodeURIComponent(new URL(url, "http://127.0.0.1").pathname);
}

async function writeStaticFile(response, absolute) {
  const info = await stat(absolute);

  if (!info.isFile()) {
    notFound(response);
    return;
  }

  response.writeHead(200, {
    "content-type": contentTypes.get(extname(absolute)) ?? "application/octet-stream",
  });
  createReadStream(absolute).pipe(response);
}

function safePath(path) {
  const absolute = normalize(join(root, path));
  if (absolute !== root && !absolute.startsWith(`${root}${sep}`)) {
    throw new Error("path escapes example root");
  }

  return absolute;
}

async function writeCertificateHash(response) {
  const path = certificatePath();

  try {
    const certificate = await readFile(path);
    const der = certificateDer(certificate);
    const hash = createHash("sha256").update(der).digest();

    response.writeHead(200, { "content-type": "application/json; charset=utf-8" });
    response.end(
      JSON.stringify({
        exists: true,
        path,
        bearerToken: authToken,
        sha256Base64: hash.toString("base64"),
        sha256Hex: hash.toString("hex"),
      }),
    );
  } catch (error) {
    response.writeHead(200, { "content-type": "application/json; charset=utf-8" });
    response.end(
      JSON.stringify({
        exists: false,
        path,
        bearerToken: authToken,
        error: error?.message ?? String(error),
      }),
    );
  }
}

function writeProtobufModule(response) {
  response.writeHead(200, { "content-type": "text/javascript; charset=utf-8" });
  response.end(`await new Promise((resolve, reject) => {
  if (globalThis.protobuf != null) {
    resolve();
    return;
  }

  const script = document.createElement("script");
  script.src = new URL("./protobuf.min.js", import.meta.url).href;
  script.onload = resolve;
  script.onerror = () => reject(new Error("failed to load protobuf.js browser bundle"));
  document.head.append(script);
});

export default globalThis.protobuf;
export const protobuf = globalThis.protobuf;
`);
}

async function writeProtobufBundle(response) {
  await writeStaticFile(response, protobufBundlePath);
}

function certificatePath() {
  return (
    process.env.TREVRPC_EXAMPLE_CERT ??
    join(homedir(), ".config", "trevrpc", "trevrpc-example-cert.pem")
  );
}

function certificateDer(certificate) {
  const text = certificate.toString("utf8");
  const match = /-----BEGIN CERTIFICATE-----([A-Za-z0-9+/=\s]+)-----END CERTIFICATE-----/.exec(
    text,
  );
  if (match == null) {
    return certificate;
  }

  return Buffer.from(match[1].replace(/\s/g, ""), "base64");
}

function notFound(response) {
  response.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
  response.end("not found");
}
