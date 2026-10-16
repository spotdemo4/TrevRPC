// Entry point of the bundle loaded by the browser interop CLI.
import { runScenarios } from "./scenarios.js";

globalThis.trevrpcInterop = { run: runScenarios };
