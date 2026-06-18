#!/usr/bin/env node

import { runGenerator } from "../src/generator.js";

const chunks = [];

for await (const chunk of process.stdin) {
  chunks.push(chunk);
}

try {
  const output = runGenerator(Buffer.concat(chunks));
  process.stdout.write(output);
} catch (error) {
  console.error(`protoc-gen-trevrpc-js: ${error.message}`);
  process.exit(1);
}
