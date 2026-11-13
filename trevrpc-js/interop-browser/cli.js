#!/usr/bin/env node
// trevrpc-interop-browser: runs the interop scenarios in a real browser (interop-cli.js), or with
// the bench subcommand the load benchmark's browser client (bench-cli.js).
//
//   trevrpc-interop-browser --browser chromium|firefox|safari --url <url> --cert-hash <hex> ...
//   trevrpc-interop-browser --write-page <dir>
//   trevrpc-interop-browser bench --browser chromium|firefox --transport <lane> --url <url> ...
//
// Arguments that do not describe a run print the command's usage on stderr and exit with status 2,
// before any browser is launched.
import { UsageError } from "./arguments.js";
import { BenchUsage, runBenchCommand } from "./bench-cli.js";
import { InteropUsage, runInteropCommand } from "./interop-cli.js";

const Interop = { name: "trevrpc-interop-browser", usage: InteropUsage, run: runInteropCommand };
const Subcommands = {
  bench: { name: "trevrpc-interop-browser bench", usage: BenchUsage, run: runBenchCommand },
};

/** Runs the command args name, resolving with its exit code. */
async function main(args) {
  const subcommand = Object.hasOwn(Subcommands, args[0]) ? Subcommands[args[0]] : undefined;
  const command = subcommand ?? Interop;
  try {
    return await command.run(subcommand ? args.slice(1) : args);
  } catch (error) {
    if (!(error instanceof UsageError)) {
      throw error;
    }
    console.error(`${command.name}: ${error.message}\n${command.usage}`);
    return 2;
  }
}

process.exit(await main(process.argv.slice(2)));
