import { existsSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";

const require = createRequire(import.meta.url);
const packageRoot = join(dirname(fileURLToPath(import.meta.url)), "..");
const SupportedTarget = "linux/x64/glibc";
const LinuxX64GnuPackage = "@trevrpc/trevrpc-js-native-linux-x64-gnu";

/** Loads the target-specific native addon without building during installation. */
export function loadNativeAddon() {
  const target = nativeTarget();
  const packageName = nativePackageName(target);
  let packageError;
  let packageState = "unsupported";

  if (packageName != null) {
    try {
      return require(packageName);
    } catch (error) {
      packageError = error;
      packageState = isMissingPackage(error, packageName) ? "missing" : "failed to load";
    }
  }

  const fallback = developmentFallbackPath();
  let fallbackError;
  if (fallback != null) {
    try {
      return require(fallback);
    } catch (error) {
      fallbackError = error;
    }
  }

  const detected = targetName(target);
  const expected = packageName ?? "no optional package (unsupported target)";
  const fallbackState =
    fallback == null
      ? "no source-checkout fallback was present"
      : fallbackError == null
        ? "the source-checkout fallback was unavailable"
        : "the source-checkout fallback failed to load";
  throw new Error(
    `TrevRPC native addon is unavailable for detected target ${detected}. ` +
      `Supported targets: ${SupportedTarget}. Expected optional package: ${expected}@0.1.0; ` +
      `package state: ${packageState}; ${fallbackState}. ` +
      "Install the matching optional package or build build/native/trevrpc_native.node explicitly in a source checkout; npm installation never starts a source build.",
    { cause: fallbackError ?? packageError },
  );
}

/** Returns the normalized native npm target. */
export function nativeTarget() {
  return Object.freeze({
    platform: process.platform,
    arch: process.arch,
    libc: process.platform === "linux" ? linuxLibc() : undefined,
  });
}

function nativePackageName(target) {
  if (target.platform === "linux" && target.arch === "x64" && target.libc === "glibc") {
    return LinuxX64GnuPackage;
  }
  return null;
}

function developmentFallbackPath() {
  if (!existsSync(join(packageRoot, "native", "CMakeLists.txt"))) {
    return null;
  }
  const path = join(packageRoot, "build", "native", "trevrpc_native.node");
  return existsSync(path) ? path : null;
}

function targetName(target) {
  return `${target.platform}/${target.arch}${target.libc == null ? "" : `/${target.libc}`}`;
}

function linuxLibc() {
  const report = process.report?.getReport?.();
  return report?.header?.glibcVersionRuntime == null ? "musl" : "glibc";
}

function isMissingPackage(error, packageName) {
  return (
    error?.code === "MODULE_NOT_FOUND" &&
    typeof error.message === "string" &&
    error.message.includes(packageName)
  );
}
