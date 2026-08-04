# Greeter example

This directory contains the generated Milestone 6 split:

- `greeter.trevrpc.js` and `.d.ts`: protobuf root, descriptors, typed clients, and factories; browser-safe.
- `greeter.node.trevrpc.js` and `.d.ts`: typed Node handler interfaces and `registerGreeterServer`.

Browser code must import only `greeter.trevrpc.js`. Node servers import registration from `greeter.node.trevrpc.js` and use `createUnaryResponse` or `createStreamingResponse` when returning metadata or a custom terminal status. A direct protobuf object such as `{ message: "hello" }` remains a protobuf response.

## Run the browser page

From `trevrpc-js`:

```sh
npm run example:greeter
```

Open `http://127.0.0.1:8080/examples/greeter/` and use a WebTransport greeter endpoint:

```text
https://127.0.0.1:50051/trevrpc  # Go default
https://127.0.0.1:5000/trevrpc   # Rust default
```

The path selects the WebTransport session; TrevRPC routing remains inside the request frame. The example policies allow origin `http://127.0.0.1:8080` and use bearer token `trevrpc-example-token`.

Both example servers write their development certificate to `~/.config/trevrpc/trevrpc-example-cert.pem`. Override it with `TREVRPC_EXAMPLE_CERT`. The static server computes the SHA-256 DER hash and the browser client passes it through `serverCertificateHashes`.

For a temporary self-signed certificate, calculate the hash with:

```sh
openssl x509 -in server.pem -outform der | openssl dgst -sha256 -binary | openssl base64
```

The client fully consumes server streams, then reads terminal metadata from `stream.status`. It calls `close()` when abandoning streams. Client `timeoutMs` is absolute across setup, upload, and receive, while `streamIdleTimeoutMs` is a separate inactivity bound.
