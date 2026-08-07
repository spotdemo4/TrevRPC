{
  lib,
  buildNpmPackage,
  importNpmLock,
  nodejs_24,
  openssl,
  protobuf,
  jq,
  binutils,
  gnugrep,
  wireGolden,
  trevrpcCSrc,
  trevrpcJsSrc,
  trevrpcC,
  trevrpcJs,
  nativePackage,
  playwright-driver,
}:
buildNpmPackage (final: {
  pname = "trevrpc-js-npm-stage";
  version = "0.1.7";

  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      wireGolden
      trevrpcCSrc
      trevrpcJsSrc
    ];
  };
  sourceRoot = "${final.src.name}/trevrpc-js";
  nodejs = nodejs_24;

  npmConfigHook = importNpmLock.npmConfigHook;
  npmDeps = importNpmLock { npmRoot = ./.; };
  dontNpmBuild = true;
  dontUseCmakeConfigure = true;

  NODE_INCLUDE_DIR = "${nodejs_24}/include/node";
  PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD = "1";
  PLAYWRIGHT_BROWSERS_PATH = "${playwright-driver.browsers}";

  nativeBuildInputs = [
    openssl
    protobuf
    jq
    binutils
    gnugrep
  ];
  buildInputs = [ trevrpcC ];

  doCheck = true;
  nativeCheckInputs = [ trevrpcJs ];
  checkPhase = ''
    runHook preCheck
    # Source checks and native lifecycle tests run once in the prerequisite package.
    test -d ${trevrpcJs}/lib/node_modules/@trevrpc/trevrpc-js
    patchShebangs bin/protoc-gen-trevrpc-js.js
    runHook postCheck
  '';

  installPhase = ''
        runHook preInstall
        export HOME="$TMPDIR/home"
        mkdir -p "$HOME" "$out"

        native_work="$TMPDIR/native-package"
        cp -R "${nativePackage}/package" "$native_work"
        chmod -R u+w "$native_work"
        # Forgejo's OIDC issuer is not trusted by npm Sigstore; ensure packed
        # tarballs never request provenance. Source package.json is already fixed,
        # but guard against future re-introduction: only CLI --provenance=false
        # reliably overrides publishConfig.provenance (env vars do not).
        ${jq}/bin/jq 'del(.publishConfig.provenance)' "$native_work/package.json" > "$native_work/package.json.tmp"
        mv "$native_work/package.json.tmp" "$native_work/package.json"
        # Also handle the core package.json in-place before packing.
        if ${jq}/bin/jq -e '.publishConfig.provenance' package.json >/dev/null 2>&1; then
          ${jq}/bin/jq 'del(.publishConfig.provenance)' package.json > package.json.tmp
          mv package.json.tmp package.json
        fi
        npm pack "$native_work" --pack-destination "$out" >/dev/null
        native_tgz="$out/trevrpc-trevrpc-js-native-linux-x64-gnu-${final.version}.tgz"
        test -f "$native_tgz"
        # Verify packed tgz does not contain provenance:true.
        ! tar -xOzf "$native_tgz" package/package.json | ${jq}/bin/jq -e '.publishConfig.provenance == true' >/dev/null 2>&1

        core_name="$(npm pack . --pack-destination "$out")"
        test "$core_name" = "trevrpc-trevrpc-js-${final.version}.tgz"
        core_tgz="$out/$core_name"
        ! tar -xOzf "$core_tgz" package/package.json | ${jq}/bin/jq -e '.publishConfig.provenance == true' >/dev/null 2>&1

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

        # Artifact checks and both npm publish dry runs happen before consumers.
        mkdir -p "$TMPDIR/verify-stage"
        ln -s "$core_tgz" "$TMPDIR/verify-stage/trevrpc-trevrpc-js-${final.version}.tgz"
        ln -s "$native_tgz" \
          "$TMPDIR/verify-stage/trevrpc-trevrpc-js-native-linux-x64-gnu-${final.version}.tgz"
        node publication-tests/verify.mjs "$TMPDIR/verify-stage" "${final.version}"

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
            protoc \
              --plugin=protoc-gen-trevrpc-js="$consumer/node_modules/.bin/protoc-gen-trevrpc-js" \
              --trevrpc-js_out="$consumer/generated" \
              --proto_path="$consumer" \
              all-shapes.proto
            cat > tsconfig.json <<'JSON'
    {
      "compilerOptions": {
        "target": "ES2022",
        "module": "NodeNext",
        "moduleResolution": "NodeNext",
        "strict": true,
        "noEmit": true,
        "skipLibCheck": false,
        "lib": ["ES2022"],
        "types": ["node"]
      },
      "include": ["types.ts", "generated/*.d.ts"]
    }
    JSON
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
          protoc \
            --plugin=protoc-gen-trevrpc-js="$browser/node_modules/.bin/protoc-gen-trevrpc-js" \
            --trevrpc-js_out="$browser/generated" \
            --proto_path="$browser" \
            all-shapes.proto
          cat > tsconfig.json <<'JSON'
    {
      "compilerOptions": {
        "target": "ES2022",
        "module": "ESNext",
        "moduleResolution": "Bundler",
        "customConditions": ["browser"],
        "strict": true,
        "noEmit": true,
        "skipLibCheck": false,
        "lib": ["ES2022", "DOM", "DOM.Iterable"],
        "types": []
      },
      "include": ["types.ts", "generated/*.d.ts"]
    }
    JSON
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

        (
          cd "$out"
          sha256sum \
            trevrpc-trevrpc-js-${final.version}.tgz \
            trevrpc-trevrpc-js-native-linux-x64-gnu-${final.version}.tgz \
            > sha256sums.txt
          jq -n \
            --arg core "$(sha256sum trevrpc-trevrpc-js-${final.version}.tgz | cut -d' ' -f1)" \
            --arg native "$(sha256sum trevrpc-trevrpc-js-native-linux-x64-gnu-${final.version}.tgz | cut -d' ' -f1)" \
            --arg version "${final.version}" \
            '{
              version: $version,
              publication: "local-stage-only",
              native_target: "linux/x64/glibc>=2.42",
              node_versions: [24],
              rpc_shapes: ["unary", "client-streaming", "server-streaming", "bidirectional-streaming"],
              browser: { bundler_types: true, esbuild: true, chromium_unary: true },
              sha256: { ("trevrpc-trevrpc-js-\($version).tgz"): $core, ("trevrpc-trevrpc-js-native-linux-x64-gnu-\($version).tgz"): $native }
            }' > manifest.json
          test "$(find . -maxdepth 1 -type f | wc -l)" -eq 4
        )

        runHook postInstall
  '';

  meta = {
    description = "Locally verified TrevRPC JavaScript npm publication stage";
    license = lib.licenses.mit;
    platforms = [ "x86_64-linux" ];
  };
})
