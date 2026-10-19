// Entry point of the bundle served by the browser interop CLI. The page fetches its input from the
// CLI, runs the scenarios and posts every result back, so a browser driver only has to open it. The
// CLI is the origin that served the page, or the origin in the `report` query parameter when a
// TrevRPC server served it.
import { runScenarios } from "./scenarios.js";

const report = new URLSearchParams(location.search).get("report") ?? "";

let posted = Promise.resolve();

/** Posts body to the CLI after every earlier post; a lost result shows up as one that never ran. */
function post(path, body) {
  posted = posted
    .then(() =>
      fetch(report + path, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(body),
      }),
    )
    .then(
      () => {},
      () => {},
    );
  return posted;
}

/** Throws unless the page and its script arrived over protocol ("h2" or "h3"), when one is set. */
function checkPageProtocol(protocol) {
  if (protocol === undefined) {
    return;
  }
  const script = new URL("page.js", location.href).href;
  const loaded = [
    performance.getEntriesByType("navigation")[0]?.nextHopProtocol,
    performance.getEntriesByType("resource").find((entry) => entry.name === script)
      ?.nextHopProtocol,
  ];
  if (loaded.some((value) => value !== protocol)) {
    throw new Error(`page and script loaded over ${loaded.join(" and ")}, want ${protocol}`);
  }
}

function describe(error) {
  return error?.message ?? String(error);
}

addEventListener("error", (event) => void post("/log", { message: `error: ${event.message}` }));
addEventListener(
  "unhandledrejection",
  (event) => void post("/log", { message: `unhandled rejection: ${describe(event.reason)}` }),
);

try {
  const input = await (await fetch(report + "/input")).json();
  checkPageProtocol(input.pageProtocol);
  await runScenarios({ ...input, onResult: (result) => void post("/result", result) });
  await post("/done", {});
} catch (error) {
  await post("/done", { error: describe(error) });
}
