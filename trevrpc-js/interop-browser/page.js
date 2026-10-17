// Entry point of the bundle served by the browser interop CLI. The page fetches its input from the
// origin that served it, runs the scenarios and posts every result back, so a browser driver only
// has to open it.
import { runScenarios } from "./scenarios.js";

let posted = Promise.resolve();

/** Posts body to the CLI after every earlier post; a lost result shows up as one that never ran. */
function post(path, body) {
  posted = posted
    .then(() =>
      fetch(path, {
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

function describe(error) {
  return error?.message ?? String(error);
}

addEventListener("error", (event) => void post("/log", { message: `error: ${event.message}` }));
addEventListener(
  "unhandledrejection",
  (event) => void post("/log", { message: `unhandled rejection: ${describe(event.reason)}` }),
);

try {
  const input = await (await fetch("/input")).json();
  await runScenarios({ ...input, onResult: (result) => void post("/result", result) });
  await post("/done", {});
} catch (error) {
  await post("/done", { error: describe(error) });
}
