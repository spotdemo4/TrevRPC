# Greeter Browser Client

This example uses `trevrpc-js` from a browser page to call the shared `example.greeter.Greeter` service implemented by the Go and Rust examples.

The browser WebTransport API can only connect to WebTransport servers. Native QUIC examples that advertise only `trevrpc/1` cannot be called directly from JavaScript.

## Run The Page

From `trevrpc-js`:

```sh
npm run example:greeter
```

Open:

```text
http://127.0.0.1:8080/examples/greeter/
```

## Server URL

Use whichever WebTransport greeter endpoint you started:

```text
https://127.0.0.1:50051/trevrpc  # Go default in this example UI
https://127.0.0.1:5000/trevrpc   # Rust default in this example UI
```

The URL path is only the WebTransport session path. TrevRPC service routing still happens inside the `RpcRequest` frame.

## WebTransport Policy

The example servers intentionally configure an explicit WebTransport policy instead of accepting every browser session:

- Path: `/trevrpc`
- Go authority: `127.0.0.1:50051` or `localhost:50051`
- Rust authority: the listen address passed to `greeter_server`, defaulting to `127.0.0.1:5000`
- Browser origin: `http://127.0.0.1:8080`

Open the page through the exact local static-server URL above so the browser sends the allowed `Origin`. Native non-browser WebTransport clients may omit `Origin`, but the authority and path still need to match.

## Authentication

The Go server, Rust server, Go client, Rust client, and JavaScript client examples all use the same bearer token:

```text
trevrpc-example-token
```

## Local Certificates

Both example servers write their local development certificate to:

```text
~/.config/trevrpc/trevrpc-example-cert.pem
```

Override the path for all examples with:

```sh
export TREVRPC_EXAMPLE_CERT=/path/to/trevrpc-example-cert.pem
```

The JavaScript static server reads that file, computes its SHA-256 DER hash, and the browser client uses that hash as `serverCertificateHashes`.

For a locally trusted certificate, leave the certificate hash field empty.

For a temporary self-signed WebTransport certificate, enter the SHA-256 digest of the DER certificate in hex or base64. For example:

```sh
openssl x509 -in server.pem -outform der | openssl dgst -sha256 -binary | openssl base64
```

Browsers enforce WebTransport certificate rules. Certificate hashes only work for short-lived self-signed certificates, so restart the example server if the generated certificate is stale. If a self-signed certificate is still rejected even with a hash, use a browser-compatible WebTransport development certificate or trust the certificate through the OS/browser trust store.
