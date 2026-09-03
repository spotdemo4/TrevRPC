# TrevRPC

[![check](https://trev.zip/llc/TrevRPC/actions/workflows/check.yaml/badge.svg?branch=main&logo=forgejo&logoColor=%23bac2de&label=check&labelColor=%23313244)](https://trev.zip/llc/TrevRPC/actions?workflow=check.yaml)
[![vulnerable](https://trev.zip/llc/TrevRPC/actions/workflows/vulnerable.yaml/badge.svg?branch=main&logo=forgejo&logoColor=%23bac2de&label=vulnerable&labelColor=%23313244)](https://trev.zip/llc/TrevRPC/actions?workflow=vulnerable.yaml)

[![c](<https://img.shields.io/badge/dynamic/regex?url=https://trev.zip/llc/TrevRPC/raw/branch/main/trevrpc-c/CMakeLists.txt&search=set%5C(CMAKE_C_STANDARD%20(.*%3F)%5C)&replace=C%241&logo=c&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%23A8B9CC>)](https://www.open-std.org/jtc1/sc22/wg14/www/standards)
[![cpp](<https://img.shields.io/badge/dynamic/regex?url=https://trev.zip/llc/TrevRPC/raw/branch/main/trevrpc-cpp/CMakeLists.txt&search=set%5C(CMAKE_CXX_STANDARD%20(.*%3F)%5C)&replace=C%2B%2B%241&logo=cplusplus&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%2300599C>)](https://isocpp.org/std/status)
[![go](<https://img.shields.io/badge/dynamic/regex?url=https://trev.zip/llc/TrevRPC/raw/branch/main/trevrpc-go/go.mod&search=go%20(.*)&replace=%241&logo=go&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%2300ADD8>)](https://go.dev/doc/devel/release)
[![javascript](https://img.shields.io/badge/dynamic/json?url=https://trev.zip/llc/TrevRPC/raw/branch/main/trevrpc-js/package.json&query=%24.engines.node&logo=javascript&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%23F7DF1E)](https://nodejs.org/en/about/previous-releases)
[![kotlin](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Ftrev.zip%2Fllc%2FTrevRPC%2Fraw%2Fbranch%2Fmain%2Ftrevrpc-kotlin%2Fgradle%2Flibs.versions.toml&query=%24.versions.kotlin&logo=kotlin&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%237F52FF)](https://kotlinlang.org/docs/releases.html)
[![rust](https://img.shields.io/badge/dynamic/toml?url=https%3A%2F%2Ftrev.zip%2Fllc%2FTrevRPC%2Fraw%2Fbranch%2Fmain%2Ftrevrpc-rust%2FCargo.toml&query=%24.package.rust-version&logo=rust&logoColor=%23bac2de&label=version&labelColor=%23313244&color=%23D34516)](https://releases.rs/)

[![pkg.go.dev](https://img.shields.io/badge/dynamic/json?url=https%3A%2F%2Fpkg.go.dev%2Fv1beta%2Fpackage%2Ftrev.zip%2Fllc%2Ftrevrpc%2Ftrevrpc-go&query=%24.version&logo=go&logoColor=%23bac2de&label=pkg.go.dev&labelColor=%23313244&color=%2300ADD8)](https://pkg.go.dev/trev.zip/llc/trevrpc/trevrpc-go)
[![npm](https://img.shields.io/npm/v/%40trevrpc%2Ftrevrpc-js?logo=npm&logoColor=%23bac2de&labelColor=%23313244&color=%23F7DF1E)](https://www.npmjs.com/package/@trevrpc/trevrpc-js)
[![maven-central](https://img.shields.io/maven-central/v/zip.trev.trevrpc/core?strategy=highestVersion&logo=apachemaven&logoColor=%23bac2de&labelColor=%23313244&color=%237F52FF)](https://central.sonatype.com/search?namespace=zip.trev.trevrpc)
[![crates.io](https://img.shields.io/crates/v/trevrpc?labelColor=%23313244&color=%23D34516)](https://crates.io/crates/trevrpc)

Protobuf over QUIC, HTTP/3 & WebTransport, with C, C++20, Go, Rust, JavaScript, and Kotlin runtimes

## Client support

| Server     |                    C                    |                     C++                     |                    Go                     |                     Rust                      |             JavaScript (Node)             |                      Kotlin                       |                       Chromium                        |                       Firefox                       |                      Safari                       |
| ---------- | :-------------------------------------: | :-----------------------------------------: | :---------------------------------------: | :-------------------------------------------: | :---------------------------------------: | :-----------------------------------------------: | :---------------------------------------------------: | :-------------------------------------------------: | :-----------------------------------------------: |
| C          |      [![C→C][smoke-c-to-c]][smoke]      |      [![C++→C][smoke-cpp-to-c]][smoke]      |      [![Go→C][smoke-go-to-c]][smoke]      |      [![Rust→C][smoke-rust-to-c]][smoke]      |      [![JS→C][smoke-js-to-c]][smoke]      |      [![Kotlin→C][smoke-kotlin-to-c]][smoke]      |      [![Chromium→C][smoke-chromium-to-c]][smoke]      |      [![Firefox→C][smoke-firefox-to-c]][smoke]      |      [![Safari→C][smoke-webkit-to-c]][smoke]      |
| C++        |    [![C→C++][smoke-c-to-cpp]][smoke]    |    [![C++→C++][smoke-cpp-to-cpp]][smoke]    |    [![Go→C++][smoke-go-to-cpp]][smoke]    |    [![Rust→C++][smoke-rust-to-cpp]][smoke]    |    [![JS→C++][smoke-js-to-cpp]][smoke]    |    [![Kotlin→C++][smoke-kotlin-to-cpp]][smoke]    |    [![Chromium→C++][smoke-chromium-to-cpp]][smoke]    |    [![Firefox→C++][smoke-firefox-to-cpp]][smoke]    |    [![Safari→C++][smoke-webkit-to-cpp]][smoke]    |
| Go         |     [![C→Go][smoke-c-to-go]][smoke]     |     [![C++→Go][smoke-cpp-to-go]][smoke]     |     [![Go→Go][smoke-go-to-go]][smoke]     |     [![Rust→Go][smoke-rust-to-go]][smoke]     |     [![JS→Go][smoke-js-to-go]][smoke]     |     [![Kotlin→Go][smoke-kotlin-to-go]][smoke]     |     [![Chromium→Go][smoke-chromium-to-go]][smoke]     |     [![Firefox→Go][smoke-firefox-to-go]][smoke]     |     [![Safari→Go][smoke-webkit-to-go]][smoke]     |
| Rust       |   [![C→Rust][smoke-c-to-rust]][smoke]   |   [![C++→Rust][smoke-cpp-to-rust]][smoke]   |   [![Go→Rust][smoke-go-to-rust]][smoke]   |   [![Rust→Rust][smoke-rust-to-rust]][smoke]   |   [![JS→Rust][smoke-js-to-rust]][smoke]   |   [![Kotlin→Rust][smoke-kotlin-to-rust]][smoke]   |   [![Chromium→Rust][smoke-chromium-to-rust]][smoke]   |   [![Firefox→Rust][smoke-firefox-to-rust]][smoke]   |   [![Safari→Rust][smoke-webkit-to-rust]][smoke]   |
| JavaScript |     [![C→JS][smoke-c-to-js]][smoke]     |     [![C++→JS][smoke-cpp-to-js]][smoke]     |     [![Go→JS][smoke-go-to-js]][smoke]     |     [![Rust→JS][smoke-rust-to-js]][smoke]     |     [![JS→JS][smoke-js-to-js]][smoke]     |     [![Kotlin→JS][smoke-kotlin-to-js]][smoke]     |     [![Chromium→JS][smoke-chromium-to-js]][smoke]     |     [![Firefox→JS][smoke-firefox-to-js]][smoke]     |     [![Safari→JS][smoke-webkit-to-js]][smoke]     |
| Kotlin     | [![C→Kotlin][smoke-c-to-kotlin]][smoke] | [![C++→Kotlin][smoke-cpp-to-kotlin]][smoke] | [![Go→Kotlin][smoke-go-to-kotlin]][smoke] | [![Rust→Kotlin][smoke-rust-to-kotlin]][smoke] | [![JS→Kotlin][smoke-js-to-kotlin]][smoke] | [![Kotlin→Kotlin][smoke-kotlin-to-kotlin]][smoke] | [![Chromium→Kotlin][smoke-chromium-to-kotlin]][smoke] | [![Firefox→Kotlin][smoke-firefox-to-kotlin]][smoke] | [![Safari→Kotlin][smoke-webkit-to-kotlin]][smoke] |

Safari interoperability issues:

- Go server: [quic-go#355](https://github.com/quic-go/webtransport-go/issues/355)
- Rust server: [h3#347](https://github.com/hyperium/h3/issues/347)

[smoke]: https://github.com/spotdemo4/TrevRPC/actions/workflows/smoke.yaml
[smoke-c-to-c]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-c-to-c&label=
[smoke-cpp-to-c]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-cpp-to-c&label=
[smoke-go-to-c]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-go-to-c&label=
[smoke-rust-to-c]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-rust-to-c&label=
[smoke-js-to-c]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-js-to-c&label=
[smoke-kotlin-to-c]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-kotlin-to-c&label=
[smoke-c-to-cpp]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-c-to-cpp&label=
[smoke-cpp-to-cpp]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-cpp-to-cpp&label=
[smoke-go-to-cpp]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-go-to-cpp&label=
[smoke-rust-to-cpp]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-rust-to-cpp&label=
[smoke-js-to-cpp]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-js-to-cpp&label=
[smoke-kotlin-to-cpp]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-kotlin-to-cpp&label=
[smoke-c-to-go]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-c-to-go&label=
[smoke-cpp-to-go]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-cpp-to-go&label=
[smoke-go-to-go]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-go-to-go&label=
[smoke-rust-to-go]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-rust-to-go&label=
[smoke-js-to-go]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-js-to-go&label=
[smoke-kotlin-to-go]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-kotlin-to-go&label=
[smoke-c-to-rust]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-c-to-rust&label=
[smoke-cpp-to-rust]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-cpp-to-rust&label=
[smoke-go-to-rust]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-go-to-rust&label=
[smoke-rust-to-rust]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-rust-to-rust&label=
[smoke-js-to-rust]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-js-to-rust&label=
[smoke-kotlin-to-rust]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-kotlin-to-rust&label=
[smoke-c-to-js]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-c-to-js&label=
[smoke-cpp-to-js]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-cpp-to-js&label=
[smoke-go-to-js]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-go-to-js&label=
[smoke-rust-to-js]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-rust-to-js&label=
[smoke-js-to-js]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-js-to-js&label=
[smoke-kotlin-to-js]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-kotlin-to-js&label=
[smoke-c-to-kotlin]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-c-to-kotlin&label=
[smoke-cpp-to-kotlin]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-cpp-to-kotlin&label=
[smoke-go-to-kotlin]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-go-to-kotlin&label=
[smoke-rust-to-kotlin]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-rust-to-kotlin&label=
[smoke-js-to-kotlin]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-js-to-kotlin&label=
[smoke-kotlin-to-kotlin]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-kotlin-to-kotlin&label=
[smoke-chromium-to-c]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-chromium-to-c&label=
[smoke-chromium-to-cpp]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-chromium-to-cpp&label=
[smoke-chromium-to-go]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-chromium-to-go&label=
[smoke-chromium-to-rust]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-chromium-to-rust&label=
[smoke-chromium-to-js]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-chromium-to-js&label=
[smoke-chromium-to-kotlin]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-chromium-to-kotlin&label=
[smoke-firefox-to-c]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-firefox-to-c&label=
[smoke-firefox-to-cpp]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-firefox-to-cpp&label=
[smoke-firefox-to-go]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-firefox-to-go&label=
[smoke-firefox-to-rust]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-firefox-to-rust&label=
[smoke-firefox-to-js]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-firefox-to-js&label=
[smoke-firefox-to-kotlin]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-firefox-to-kotlin&label=
[smoke-webkit-to-c]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-webkit-to-c&label=
[smoke-webkit-to-cpp]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-webkit-to-cpp&label=
[smoke-webkit-to-go]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-webkit-to-go&label=
[smoke-webkit-to-rust]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-webkit-to-rust&label=
[smoke-webkit-to-js]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-webkit-to-js&label=
[smoke-webkit-to-kotlin]: https://img.shields.io/github/check-runs/spotdemo4/TrevRPC/main?nameFilter=smoke-webkit-to-kotlin&label=

Full documentation is available in the [TrevRPC wiki](https://trev.zip/llc/TrevRPC/wiki)
