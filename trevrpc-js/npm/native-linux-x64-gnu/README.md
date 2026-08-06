# @trevrpc/trevrpc-js-native-linux-x64-gnu

Target-specific native addon used by `@trevrpc/trevrpc-js@0.1.4`.

Supported runtime contract:

- Linux x86-64
- glibc 2.42 or newer
- Node.js 24

Install `@trevrpc/trevrpc-js`; its exact optional dependency selects this package automatically. Do not import this package directly. The package contains one N-API `.node` addon and its `$ORIGIN`-resolved `libmsquic.so.2` dependency.
