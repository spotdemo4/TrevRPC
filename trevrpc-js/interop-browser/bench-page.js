// Entry point of the load benchmark's page: fetches the run's input from the CLI, runs it and
// posts the result line back as the body of /done.
import { runBench } from "./bench.js";
import { describe, post, readInput } from "./report.js";

try {
  const input = await readInput();
  if (!crossOriginIsolated) {
    // Timers are then coarser: about 100 µs in Chromium instead of 5 µs.
    await post("/log", { message: "the page is not cross-origin isolated" });
  }
  const onError = (error) => void post("/log", { message: `first failure: ${describe(error)}` });
  await post("/done", { result: await runBench(input, { onError }) });
} catch (error) {
  await post("/done", { error: describe(error) });
}
