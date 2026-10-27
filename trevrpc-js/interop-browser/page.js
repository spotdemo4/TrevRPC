// Entry point of the bundle served by the browser interop CLI. The page fetches its input from the
// CLI, runs the scenarios and posts every result back, so a browser driver only has to open it.
import { describe, post, readInput } from "./report.js";
import { runScenarios } from "./scenarios.js";

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

try {
  const input = await readInput();
  checkPageProtocol(input.pageProtocol);
  await runScenarios({ ...input, onResult: (result) => void post("/result", result) });
  await post("/done", {});
} catch (error) {
  await post("/done", { error: describe(error) });
}
