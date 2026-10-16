# TrevRPC

[![check](https://trev.zip/llc/TrevRPC/actions/workflows/check.yaml/badge.svg?branch=main&logo=forgejo&logoColor=%23bac2de&label=check&labelColor=%23313244)](https://trev.zip/llc/TrevRPC/actions?workflow=check.yaml)
[![vulnerable](https://trev.zip/llc/TrevRPC/actions/workflows/vulnerable.yaml/badge.svg?branch=main&logo=forgejo&logoColor=%23bac2de&label=vulnerable&labelColor=%23313244)](https://trev.zip/llc/TrevRPC/actions?workflow=vulnerable.yaml)
[![android](https://img.shields.io/github/actions/workflow/status/spotdemo4/TrevRPC/android.yaml?branch=main&logo=android&logoColor=%23bac2de&label=android&labelColor=%23313244)](https://github.com/spotdemo4/TrevRPC/actions/workflows/android.yaml)
[![safari](https://img.shields.io/github/actions/workflow/status/spotdemo4/TrevRPC/safari.yaml?branch=main&logo=safari&logoColor=%23bac2de&label=safari&labelColor=%23313244)](https://github.com/spotdemo4/TrevRPC/actions/workflows/safari.yaml)

[![go](<https://img.shields.io/badge/dynamic/regex?url=https://trev.zip/llc/TrevRPC/raw/branch/main/trevrpc-go/go.mod&search=go%20(.*)&replace=%241&logo=go&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%2300ADD8>)](https://go.dev/doc/devel/release)
[![javascript](https://img.shields.io/badge/dynamic/json?url=https://trev.zip/llc/TrevRPC/raw/branch/main/trevrpc-js/package.json&query=%24.engines.node&logo=javascript&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%23F7DF1E)](https://nodejs.org/en/about/previous-releases)
[![kotlin](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Ftrev.zip%2Fllc%2FTrevRPC%2Fraw%2Fbranch%2Fmain%2Ftrevrpc-kotlin%2Fgradle%2Flibs.versions.toml&query=%24.versions.kotlin&logo=kotlin&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%237F52FF)](https://kotlinlang.org/docs/releases.html)
[![rust](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Ftrev.zip%2Fllc%2FTrevRPC%2Fraw%2Fbranch%2Fmain%2Ftrevrpc-rust%2FCargo.toml&query=%24.package.rust-version&logo=rust&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%23D34516)](https://releases.rs/)

[![pkg.go.dev](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fpkg.go.dev%2Fv1beta%2Fpackage%2Ftrev.zip%2Fllc%2Ftrevrpc%2Ftrevrpc-go&query=%24.version&logo=go&logoColor=%23bac2de&label=pkg.go.dev&labelColor=%23313244&color=%2300ADD8)](https://pkg.go.dev/trev.zip/llc/trevrpc/trevrpc-go)
[![npm](https://img.shields.io/npm/v/%40trevrpc%2Ftrevrpc-js?logo=npm&logoColor=%23bac2de&labelColor=%23313244&color=%23F7DF1E)](https://www.npmjs.com/package/@trevrpc/trevrpc-js)
[![maven-central](https://img.shields.io/maven-central/v/zip.trev.trevrpc/core?strategy=highestVersion&logo=apachemaven&logoColor=%23bac2de&labelColor=%23313244&color=%237F52FF)](https://central.sonatype.com/search?namespace=zip.trev.trevrpc)
[![crates.io](https://img.shields.io/crates/v/trevrpc?labelColor=%23313244&color=%23D34516)](https://crates.io/crates/trevrpc)

Protobuf over QUIC, HTTP/3 & WebTransport, with Go, Rust, JavaScript, and Kotlin runtimes

## Support

| Runtime                            | Server                                | Client       |
| ---------------------------------- | ------------------------------------- | ------------ |
| [Go](trevrpc-go)                   | native QUIC, HTTP/3, and WebTransport | native QUIC  |
| [Rust](trevrpc-rust)               | native QUIC, HTTP/3, and WebTransport | native QUIC  |
| [JavaScript](trevrpc-js) (browser) | —                                     | WebTransport |
| [Kotlin](trevrpc-kotlin) (Android) | —                                     | HTTP/3       |

Every client is tested against every server; see [interop](interop). The
browser client is supported on Chromium and Firefox. Safari is not currently
supported: neither `webtransport-go` nor Rust's `h3` interoperates with its
WebTransport profile ([h3#347](https://github.com/hyperium/h3/issues/347)). The
`safari` workflow keeps running against both servers so that it shows when
upstream compatibility returns.

The wire format is shared by every runtime and pinned by
[`testdata/wire-golden-vectors.txt`](testdata/wire-golden-vectors.txt).

Full documentation is available in the [TrevRPC wiki](https://trev.zip/llc/TrevRPC/wiki)
