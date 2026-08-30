{
  lib,
  stdenv,
  buildNpmPackage,
  importNpmLock,
  jsPackage,
  nodejs_24,
  cmake,
  clang-tools,
  openssl,
  oxfmt,
  oxlint,
  protobuf,
  gnugrep,
  makeWrapper,
  packageManifestWriter,
  playwright-driver,
  benchProto,
  wireGolden,
  trevrpcC,
  nativePackage ? null,
}:
let
  publication = stdenv.hostPlatform.system == "x86_64-linux" && nativePackage != null;
  nativeNpmPackage = if publication then nativePackage.npm else null;
in
buildNpmPackage (final: {
  pname = "trevrpc-js";
  inherit (jsPackage) version;
  outputs = [ "out" ] ++ lib.optional publication "npm";

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      benchProto
      wireGolden
      ./.
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-js";
  nodejs = nodejs_24;

  npmConfigHook = importNpmLock.npmConfigHook;
  npmDeps = importNpmLock {
    npmRoot = ./.;
  };

  npmBuildScript = "build:native";
  dontUseCmakeConfigure = true;
  NODE_INCLUDE_DIR = "${nodejs_24}/include/node";
  TREVRPC_NIX_PUBLICATION = "1";
  PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = "1";
  PLAYWRIGHT_BROWSERS_PATH = lib.optionalString publication "${playwright-driver.browsers}";

  nativeBuildInputs = [
    cmake
    makeWrapper
  ];
  buildInputs = [
    trevrpcC
  ];

  doCheck = true;
  nativeCheckInputs = [
    clang-tools
    openssl
    oxfmt
    oxlint
    protobuf
  ];
  checkPhase = ''
    runHook preCheck
    rm -rf build
    oxfmt --check
    oxlint --deny-warnings
    npm run typecheck
    npm run build:native:test
    clang-format --dry-run --Werror native/trevrpc_node.c
    clang-tidy -p build/native native/trevrpc_node.c
    cp bin/protoc-gen-trevrpc-js.js "$TMPDIR/protoc-gen-trevrpc-js.js"
    patchShebangs bin/protoc-gen-trevrpc-js.js
    npm test
    cp "$TMPDIR/protoc-gen-trevrpc-js.js" bin/protoc-gen-trevrpc-js.js
    rm -rf bench/node_modules
    mkdir -p bench/node_modules/@trevrpc
    ln -s "$PWD" bench/node_modules/@trevrpc/trevrpc-js
    npm --prefix bench test
    rm -rf bench/node_modules
    npm run build:native
    npm run verify:native:production
    ! grep -q '@grpc/' package.json
    runHook postCheck
  '';

  preInstall = lib.optionalString publication ''
    cp -R node_modules "$TMPDIR/publication-node_modules"
  '';

  postInstall = ''
    native_dir="$out/lib/node_modules/@trevrpc/trevrpc-js/node_modules/@trevrpc/trevrpc-js-native-linux-x64-gnu"
    ${lib.optionalString publication ''
      mkdir -p "$(dirname "$native_dir")"
      cp -R "${nativePackage}/package" "$native_dir"
      installed_manifest="$out/lib/node_modules/@trevrpc/trevrpc-js/package.json"
      chmod u+w "$installed_manifest"
      node ${packageManifestWriter} core "$installed_manifest" "${final.version}" \
        npm/native-linux-x64-gnu/package.template.json
    ''}
    ${lib.optionalString stdenv.hostPlatform.isDarwin ''
      install -Dm755 build/native/trevrpc_native.node \
        "$out/lib/node_modules/@trevrpc/trevrpc-js/native/trevrpc_native.node"
    ''}

    mkdir -p "$out/lib/node_modules/trevrpc-bench-peer-js/node_modules/@trevrpc"
    cp bench/package.json bench/common.js bench/trevrpc-bench-peer.js \
      "$out/lib/node_modules/trevrpc-bench-peer-js/"
    ln -s "$out/lib/node_modules/@trevrpc/trevrpc-js" \
      "$out/lib/node_modules/trevrpc-bench-peer-js/node_modules/@trevrpc/trevrpc-js"
    makeWrapper ${nodejs_24}/bin/node "$out/bin/trevrpc-bench-peer-js" \
      --add-flags "$out/lib/node_modules/trevrpc-bench-peer-js/trevrpc-bench-peer.js"

    cp -R conformance "$out/lib/node_modules/@trevrpc/trevrpc-js/conformance"
    makeWrapper ${nodejs_24}/bin/node "$out/bin/trevrpc-conformance-js" \
      --add-flags "--no-addons" \
      --add-flags "$out/lib/node_modules/@trevrpc/trevrpc-js/conformance/trevrpc-conformance-js.js"

    ${lib.optionalString publication ''
      export HOME="$TMPDIR/home"
      mkdir -p "$HOME" "$npm"

      node ${packageManifestWriter} core package.json "${final.version}" \
        npm/native-linux-x64-gnu/package.template.json

      core_name="$(npm pack . --silent --pack-destination "$npm")"
      test "$core_name" = "trevrpc-trevrpc-js-${final.version}.tgz"
      core_tgz="$npm/$core_name"
      test -f "$core_tgz"

      native_name="trevrpc-trevrpc-js-native-linux-x64-gnu-${final.version}.tgz"
      cp "${nativeNpmPackage}/$native_name" "$npm/$native_name"
      native_tgz="$npm/$native_name"
      test -f "$native_tgz"

      node publication-tests/verify.mjs stage "$npm" "${final.version}"
      test "$(find "$npm" -maxdepth 1 -type f | wc -l)" -eq 4
    ''}
  '';

  doInstallCheck = true;
  nativeInstallCheckInputs = [
    gnugrep
  ]
  ++ lib.optionals publication [
    openssl
    protobuf
  ];
  installCheckPhase = ''
    runHook preInstallCheck
    loader="$out/lib/node_modules/@trevrpc/trevrpc-js/src/native-loader.js"
    ${lib.optionalString stdenv.hostPlatform.isDarwin ''
      expected_native="$out/lib/node_modules/@trevrpc/trevrpc-js/native/trevrpc_native.node"
      test -f "$expected_native"
      test ! -e "$out/lib/node_modules/@trevrpc/trevrpc-js/node_modules/@trevrpc/trevrpc-js-native-linux-x64-gnu"
    ''}
    ${lib.optionalString publication ''
      expected_native="$out/lib/node_modules/@trevrpc/trevrpc-js/node_modules/@trevrpc/trevrpc-js-native-linux-x64-gnu/trevrpc_native.node"
      test -f "$expected_native"
      test ! -e "$out/lib/node_modules/@trevrpc/trevrpc-js/native/trevrpc_native.node"
    ''}
    ${lib.optionalString (stdenv.hostPlatform.isDarwin || publication) ''
      ${nodejs_24}/bin/node --input-type=module -e '
        import assert from "node:assert/strict";
        import { createRequire } from "node:module";
        import { pathToFileURL } from "node:url";
        const require = createRequire(import.meta.url);
        const { loadNativeAddon } = await import(pathToFileURL(process.argv[1]));
        assert.strictEqual(loadNativeAddon(), require(process.argv[2]));
      ' "$loader" "$expected_native"
    ''}
    installed_generator="$out/lib/node_modules/@trevrpc/trevrpc-js/bin/protoc-gen-trevrpc-js.js"
    IFS= read -r installed_shebang < "$installed_generator"
    test "$installed_shebang" = "#!${nodejs_24}/bin/node"
    test -x "$out/bin/trevrpc-bench-peer-js"
    test -x "$out/bin/trevrpc-conformance-js"
    printf 'STOP\n' | "$out/bin/trevrpc-conformance-js" --protocol 1 > peer.out
    grep -q '"event":"ready"' peer.out
    grep -q '"peer":"js"' peer.out

    ${lib.optionalString publication ''
      rm -rf node_modules
      mv "$TMPDIR/publication-node_modules" node_modules

      core_tgz="$npm/trevrpc-trevrpc-js-${final.version}.tgz"
      native_tgz="$npm/trevrpc-trevrpc-js-native-linux-x64-gnu-${final.version}.tgz"
      node publication-tests/verify.mjs core "$core_tgz" "${final.version}"

      dependency_tarballs="$TMPDIR/dependency-tarballs"
      dependency_work="$TMPDIR/dependency-pack-work"
      mkdir -p "$dependency_tarballs" \
        "$dependency_work/protobufjs/package" "$dependency_work/long/package" \
        "$dependency_work/node-types/package" "$dependency_work/undici-types/package"
      cp -R node_modules/protobufjs/. "$dependency_work/protobufjs/package/"
      cp -R node_modules/long/. "$dependency_work/long/package/"
      cp -R node_modules/@types/node/. "$dependency_work/node-types/package/"
      cp -R node_modules/undici-types/. "$dependency_work/undici-types/package/"
      protobufjs_tgz="$dependency_tarballs/protobufjs-8.7.1.tgz"
      long_tgz="$dependency_tarballs/long-5.3.2.tgz"
      node_types_tgz="$dependency_tarballs/types-node-24.13.3.tgz"
      undici_types_tgz="$dependency_tarballs/undici-types-7.18.2.tgz"
      tar -czf "$protobufjs_tgz" -C "$dependency_work/protobufjs" package
      tar -czf "$long_tgz" -C "$dependency_work/long" package
      tar -czf "$node_types_tgz" -C "$dependency_work/node-types" package
      tar -czf "$undici_types_tgz" -C "$dependency_work/undici-types" package

      test_cert="$TMPDIR/server-cert.pem"
      test_key="$TMPDIR/server-key.pem"
      openssl req -x509 -newkey rsa:2048 -nodes -days 1 \
        -subj /CN=localhost -keyout "$test_key" -out "$test_cert" >/dev/null 2>&1

      tsc="$PWD/node_modules/.bin/tsc"
      esbuild="$PWD/node_modules/.bin/esbuild"
      for runtime in "${nodejs_24}"; do
        consumer="$TMPDIR/node-$(basename "$runtime")"
        mkdir -p "$consumer/generated"
        printf '%s\n' '{"private":true,"type":"module"}' > "$consumer/package.json"
        cp publication-tests/node/loopback.mjs publication-tests/node/types.ts "$consumer/"
        cp publication-tests/proto/all-shapes.proto "$consumer/"
        (
          cd "$consumer"
          export PATH="$runtime/bin:$PATH"
          npm install --offline --ignore-scripts \
            "$core_tgz" "$native_tgz" "$protobufjs_tgz" "$long_tgz" \
            "$node_types_tgz" "$undici_types_tgz" >/dev/null
          patchShebangs \
            "$consumer/node_modules/@trevrpc/trevrpc-js/bin/protoc-gen-trevrpc-js.js"
          protoc \
            --plugin=protoc-gen-trevrpc-js="$consumer/node_modules/.bin/protoc-gen-trevrpc-js" \
            --trevrpc-js_out="$consumer/generated" \
            --proto_path="$consumer" \
            all-shapes.proto
          printf '%s\n' \
            '{' \
            '  "compilerOptions": {' \
            '    "target": "ES2022",' \
            '    "module": "NodeNext",' \
            '    "moduleResolution": "NodeNext",' \
            '    "strict": true,' \
            '    "noEmit": true,' \
            '    "skipLibCheck": false,' \
            '    "lib": ["ES2022"],' \
            '    "types": ["node"]' \
            '  },' \
            '  "include": ["types.ts", "generated/*.d.ts"]' \
            '}' > tsconfig.json
          "$tsc" -p tsconfig.json
          TREVRPC_TEST_CERT="$test_cert" TREVRPC_TEST_KEY="$test_key" \
            "$runtime/bin/node" loopback.mjs
        )
      done

      browser="$TMPDIR/browser-consumer"
      mkdir -p "$browser/generated"
      printf '%s\n' '{"private":true,"type":"module"}' > "$browser/package.json"
      cp publication-tests/browser/entry.js publication-tests/browser/types.ts "$browser/"
      cp publication-tests/proto/all-shapes.proto "$browser/"
      (
        cd "$browser"
        npm install --offline --ignore-scripts --omit=optional \
          "$core_tgz" "$protobufjs_tgz" "$long_tgz" >/dev/null
        patchShebangs \
          "$browser/node_modules/@trevrpc/trevrpc-js/bin/protoc-gen-trevrpc-js.js"
        protoc \
          --plugin=protoc-gen-trevrpc-js="$browser/node_modules/.bin/protoc-gen-trevrpc-js" \
          --trevrpc-js_out="$browser/generated" \
          --proto_path="$browser" \
          all-shapes.proto
        printf '%s\n' \
          '{' \
          '  "compilerOptions": {' \
          '    "target": "ES2022",' \
          '    "module": "ESNext",' \
          '    "moduleResolution": "Bundler",' \
          '    "customConditions": ["browser"],' \
          '    "strict": true,' \
          '    "noEmit": true,' \
          '    "skipLibCheck": false,' \
          '    "lib": ["ES2022", "DOM", "DOM.Iterable"],' \
          '    "types": []' \
          '  },' \
          '  "include": ["types.ts", "generated/*.d.ts"]' \
          '}' > tsconfig.json
        "$tsc" -p tsconfig.json
        "$esbuild" entry.js \
          --bundle \
          --format=esm \
          --platform=browser \
          --conditions=browser,import \
          --outfile=bundle.js \
          --metafile=metafile.json
        ! grep -a -E 'node:|trevrpc_native|native-loader|@trevrpc/trevrpc-js-native' \
          bundle.js metafile.json
      )

      for chromium in "$PLAYWRIGHT_BROWSERS_PATH"/chromium-*/chrome-linux*/chrome; do
        if test -x "$chromium"; then
          export TREVRPC_BROWSER_CHROMIUM="$chromium"
          break
        fi
      done
      test -n "''${TREVRPC_BROWSER_CHROMIUM:-}"
      node publication-tests/browser/smoke.mjs "$browser/bundle.js"
      node publication-tests/verify.mjs pair "$npm" "${final.version}"
    ''}

    runHook postInstallCheck
  '';

  meta = {
    mainProgram = "protoc-gen-trevrpc-js";
    description = "JavaScript WebTransport runtime and protobuf.js code generator for TrevRPC";
    license = lib.licenses.mit;
    platforms = lib.platforms.all;
    badPlatforms = [ lib.systems.inspect.platformPatterns.isStatic ];
    homepage = "https://trev.zip/llc/TrevRPC";
    changelog = "https://trev.zip/llc/TrevRPC/releases";
    downloadPage = "https://trev.zip/llc/TrevRPC/releases/tag/v${final.version}";
  };
})
