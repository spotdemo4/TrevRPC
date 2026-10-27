// How a page talks to the CLI that drives it: it fetches its input and posts what it finds back.
// The CLI is the origin that served the page, or the origin in the `report` query parameter when a
// TrevRPC server served it. Importing this module also reports uncaught errors.
const report = new URLSearchParams(location.search).get("report") ?? "";

let posted = Promise.resolve();

/** Posts body to the CLI after every earlier post; a lost result shows up as one that never ran. */
export function post(path, body) {
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

/** The page's input from the CLI. */
export async function readInput() {
  return (await fetch(report + "/input")).json();
}

export function describe(error) {
  return error?.message ?? String(error);
}

addEventListener("error", (event) => void post("/log", { message: `error: ${event.message}` }));
addEventListener(
  "unhandledrejection",
  (event) => void post("/log", { message: `unhandled rejection: ${describe(event.reason)}` }),
);
