import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import { createServer } from "node:http";
import { extname, join, normalize, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(fileURLToPath(new URL("../..", import.meta.url)));
const port = Number.parseInt(process.env.PORT ?? "8080", 10);

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
    const file = path.endsWith("/") ? join(path, "index.html") : path;
    const absolute = safePath(file);
    const info = await stat(absolute);

    if (!info.isFile()) {
      notFound(response);
      return;
    }

    response.writeHead(200, {
      "content-type": contentTypes.get(extname(absolute)) ?? "application/octet-stream",
    });
    createReadStream(absolute).pipe(response);
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

function safePath(path) {
  const absolute = normalize(join(root, path));
  if (absolute !== root && !absolute.startsWith(`${root}${sep}`)) {
    throw new Error("path escapes example root");
  }

  return absolute;
}

function notFound(response) {
  response.writeHead(404, { "content-type": "text/plain; charset=utf-8" });
  response.end("not found");
}
