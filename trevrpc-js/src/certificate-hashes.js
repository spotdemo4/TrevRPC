import { unavailable } from "./status.js";

/** The path that follows the `TrevRPC` endpoint's path where a development server publishes hashes. */
const CertificateHashesPath = "/certificate-hashes";

/**
 * Where a channel fetches certificate hashes from before each WebTransport attempt, or `undefined`
 * when it does not. `fetchServerCertificateHashes: true` derives the URL from the channel's own,
 * which is where a server with a development certificate publishes them, so the page needs no
 * proxy or route of its own; `serverCertificateHashesUrl` names another location. Setting both is
 * ambiguous and throws a TypeError; `fetchServerCertificateHashes: false` leaves the custom URL in
 * effect.
 */
export function certificateHashesUrl(channelUrl, options) {
  const { fetchServerCertificateHashes: derive, serverCertificateHashesUrl: custom } = options;
  if (derive === true && custom !== undefined) {
    throw new TypeError(
      "fetchServerCertificateHashes and serverCertificateHashesUrl are mutually exclusive",
    );
  }
  if (derive !== true) {
    return custom;
  }
  const url = new URL(channelUrl);
  url.pathname = url.pathname.replace(/\/+$/u, "") + CertificateHashesPath;
  url.search = "";
  url.hash = "";
  return url.href;
}

/**
 * Fetches the certificate hashes a TrevRPC development server publishes, as WebTransport
 * `serverCertificateHashes`. The server answers JSON such as
 * `[{"algorithm":"sha-256","value":"<hex>"}]`; a 404 means it has a certificate the browser trusts
 * already, and yields no hashes. Any other failure rejects with Unavailable.
 */
export async function fetchServerCertificateHashes(url, { signal, fetch = globalThis.fetch } = {}) {
  if (typeof fetch !== "function") {
    throw unavailable("fetch is not available in this JavaScript runtime");
  }
  let response;
  try {
    response = await fetch(url, { signal, cache: "no-store" });
  } catch (error) {
    if (signal?.aborted) {
      throw error;
    }
    throw unavailable(`cannot fetch server certificate hashes: ${error?.message ?? error}`, error);
  }
  if (response.status === 404) {
    return [];
  }
  if (!response.ok) {
    throw unavailable(`cannot fetch server certificate hashes: HTTP ${response.status}`);
  }
  let entries;
  try {
    entries = await response.json();
  } catch (error) {
    if (signal?.aborted) {
      throw error;
    }
    throw unavailable("server certificate hashes are not JSON", error);
  }
  if (!Array.isArray(entries)) {
    throw unavailable("server certificate hashes are not a JSON array");
  }
  return entries.map(parseHash);
}

function parseHash(entry) {
  const { algorithm, value } = entry ?? {};
  if (
    typeof algorithm !== "string" ||
    typeof value !== "string" ||
    !/^(?:[0-9a-f]{2})+$/iu.test(value)
  ) {
    throw unavailable(`invalid server certificate hash ${JSON.stringify(entry)}`);
  }
  const bytes = new Uint8Array(value.length / 2);
  for (let index = 0; index < bytes.length; index += 1) {
    bytes[index] = Number.parseInt(value.slice(index * 2, index * 2 + 2), 16);
  }
  return { algorithm, value: bytes };
}
