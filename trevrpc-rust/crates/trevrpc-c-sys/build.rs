use std::env;
use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=DOCS_RS");
    println!("cargo:rerun-if-env-changed=PKG_CONFIG");
    println!("cargo:rerun-if-env-changed=PKG_CONFIG_PATH");
    println!("cargo:rerun-if-env-changed=PKG_CONFIG_LIBDIR");
    println!("cargo:rerun-if-env-changed=PKG_CONFIG_SYSROOT_DIR");
    println!("cargo:rerun-if-env-changed=TREVRPC_NATIVE_FFI_SANITIZERS");

    if env::var_os("DOCS_RS").is_some()
        || (env::var_os("CARGO_FEATURE_SYSTEM_NATIVE").is_none()
            && env::var_os("CARGO_FEATURE_TESTING").is_none())
    {
        return;
    }

    let target_os = env::var("CARGO_CFG_TARGET_OS").expect("Cargo did not set CARGO_CFG_TARGET_OS");
    let target_arch =
        env::var("CARGO_CFG_TARGET_ARCH").expect("Cargo did not set CARGO_CFG_TARGET_ARCH");
    if !matches!(target_os.as_str(), "linux" | "macos")
        || !matches!(target_arch.as_str(), "x86_64" | "aarch64")
    {
        return;
    }

    let pkg_config = env::var_os("PKG_CONFIG").unwrap_or_else(|| "pkg-config".into());
    if env::var_os("CARGO_FEATURE_SYSTEM_NATIVE").is_some() {
        emit_package_link_flags(Path::new(&pkg_config), "trevrpc_transport_msquic");
    }
    if env::var_os("CARGO_FEATURE_TESTING").is_some() {
        emit_package_link_flags(Path::new(&pkg_config), "trevrpc_transport_testing");
    }
    if target_os == "linux" && env::var_os("TREVRPC_NATIVE_FFI_SANITIZERS").is_some() {
        println!("cargo:rustc-link-lib=asan");
        println!("cargo:rustc-link-lib=ubsan");
    }
}

fn emit_package_link_flags(pkg_config: &Path, package: &str) {
    let output = Command::new(pkg_config)
        .args(["--static", "--libs", package])
        .output()
        .unwrap_or_else(|error| {
            panic!(
                "failed to execute {}: {error}; install pkg-config and the {package} development package, or set PKG_CONFIG and PKG_CONFIG_PATH",
                pkg_config.display()
            )
        });
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "pkg-config could not resolve the static {package} closure: {}; install the matching TrevRPC C Transport development package and set PKG_CONFIG_PATH",
            stderr.trim()
        );
    }

    let flags = String::from_utf8(output.stdout).expect(
        "pkg-config returned non-UTF-8 linker flags for the selected TrevRPC transport package",
    );
    emit_link_flags(&flags);
}

fn emit_link_flags(flags: &str) {
    let mut arguments = flags.split_whitespace();
    while let Some(argument) = arguments.next() {
        if let Some(path) = argument.strip_prefix("-L") {
            println!("cargo:rustc-link-search=native={path}");
        } else if let Some(name) = argument.strip_prefix("-l") {
            println!("cargo:rustc-link-lib={name}");
        } else if let Some(path) = argument.strip_prefix("-F") {
            println!("cargo:rustc-link-search=framework={path}");
        } else if argument == "-framework" {
            let framework = arguments
                .next()
                .expect("pkg-config emitted -framework without a framework name");
            println!("cargo:rustc-link-lib=framework={framework}");
        } else if argument == "-pthread"
            || argument.starts_with("-Wl,")
            || Path::new(argument).extension().is_some_and(|extension| {
                ["a", "so", "dylib"]
                    .iter()
                    .any(|candidate| extension.eq_ignore_ascii_case(candidate))
            })
        {
            println!("cargo:rustc-link-arg={argument}");
        }
    }
}
