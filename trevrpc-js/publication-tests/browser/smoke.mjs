import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

import { chromium } from "playwright";

const browser = await chromium.launch({
  executablePath: process.env.TREVRPC_BROWSER_CHROMIUM,
  headless: true,
});
try {
  const page = await browser.newPage();
  await page.addInitScript(() => {
    const response = new Uint8Array([
      0, 0, 0, 11, 0x1a, 9, 0x0a, 7, 0x62, 0x72, 0x6f, 0x77, 0x73, 0x65, 0x72,
    ]);
    globalThis.WebTransport = class FakeWebTransport {
      ready = Promise.resolve();
      closed = new Promise(() => {});
      close() {}
      async createBidirectionalStream() {
        return {
          readable: new ReadableStream({
            start(controller) {
              controller.enqueue(response);
              controller.close();
            },
          }),
          writable: new WritableStream(),
        };
      }
    };
  });
  const bundle = await readFile(process.argv[2], "utf8");
  await page.goto("data:text/html,<meta charset=utf-8>");
  await page.addScriptTag({ content: bundle, type: "module" });
  const result = await page.evaluate(() => globalThis.trevrpcBrowserSmoke);
  assert.equal(result, "browser");
} finally {
  await browser.close();
}
