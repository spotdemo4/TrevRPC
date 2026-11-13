// What the commands of trevrpc-interop-browser share to read their arguments. A parser throws
// UsageError rather than exiting, so that the tests can call it in-process, and cli.js prints the
// command's usage and exits with status 2.
import { parseArgs } from "node:util";

/** Arguments that do not describe a run: the command prints message with its usage. */
export class UsageError extends Error {
  name = "UsageError";
}

/** The values of the options in args, which must all be among options (parseArgs's). */
export function parseOptions(args, options) {
  try {
    return parseArgs({ args, options, strict: true }).values;
  } catch (error) {
    throw new UsageError(error.message, { cause: error });
  }
}

/** The certificate hash in hex as bytes, or a UsageError unless it is a SHA-256 in hex. */
export function parseCertHash(hex) {
  if (!/^(?:[0-9a-fA-F]{2}){32}$/.test(hex ?? "")) {
    throw new UsageError(
      "--cert-hash must be the hex SHA-256 of the certificate DER (64 hex digits)",
    );
  }
  return [...Buffer.from(hex, "hex")];
}
