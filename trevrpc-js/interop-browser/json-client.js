// The load benchmark's JSON client: the baseline's JSON API (bench/baseline/echo_json.go) called
// with plain fetch, as most web apps call their server, with no RPC framework. Its methods match a
// connect-web Echo client's, so the benchmark runs either the same way.

/**
 * A client of the JSON API at baseUrl. unary(request, { signal, headers }) resolves with the
 * response; serverStream(request, { signal, headers }) yields each response of the
 * newline-delimited JSON body as it arrives.
 */
export function createJsonClient(baseUrl, fetchImpl = globalThis.fetch) {
  const post = async (path, request, { signal, headers }) => {
    const response = await fetchImpl(`${baseUrl}${path}`, {
      method: "POST",
      headers: { "content-type": "application/json", ...headers },
      body: JSON.stringify(request),
      signal,
    });
    if (!response.ok) {
      throw new Error(`${path} answered ${response.status}: ${await response.text()}`);
    }
    return response;
  };
  return {
    unary: async (request, options = {}) => (await post("/json/unary", request, options)).json(),
    async *serverStream(request, options = {}) {
      const response = await post("/json/server-stream", request, options);
      yield* jsonLines(response.body);
    },
  };
}

/** Each line of a newline-delimited JSON body, parsed; a last line may lack its newline. */
export async function* jsonLines(body) {
  const reader = body.pipeThrough(new TextDecoderStream()).getReader();
  let buffered = "";
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) {
        break;
      }
      buffered += value;
      let start = 0;
      for (let end; (end = buffered.indexOf("\n", start)) !== -1; start = end + 1) {
        yield JSON.parse(buffered.slice(start, end));
      }
      buffered = buffered.slice(start);
    }
    if (buffered.trim() !== "") {
      yield JSON.parse(buffered);
    }
  } finally {
    // Stops the download when the caller stops reading early.
    reader.cancel().catch(() => {});
  }
}
