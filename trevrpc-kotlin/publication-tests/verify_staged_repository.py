#!/usr/bin/env python3
"""Validate the repository-local TrevRPC Kotlin Maven staging repository."""

from __future__ import annotations

import json
import os
import pathlib
import struct
import subprocess
import sys
import zipfile
import xml.etree.ElementTree as ET

GROUP = "zip.trev.trevrpc"
VERSION = "0.1.0"
MODULE_TARGETS = {
    "core": 52,
    "transport-cronet": 61,
    "transport-netty": 65,
    "protoc-gen-trevrpc-kotlin": 65,
}
TARGET_JVM = {
    "core": 8,
    "transport-cronet": 17,
    "transport-netty": 21,
    "protoc-gen-trevrpc-kotlin": 21,
}
NATIVE_CLASSIFIERS = {
    "linux-x86_64",
    "linux-aarch_64",
    "osx-x86_64",
    "osx-aarch_64",
    "windows-x86_64",
}
POM_NAMESPACE = {"m": "http://maven.apache.org/POM/4.0.0"}
GENERATOR_THIRD_PARTY_COORDINATES = [
    "com.google.protobuf:protobuf-java:4.35.1",
    "org.jetbrains:annotations:13.0",
    "org.jetbrains.kotlin:kotlin-stdlib:2.4.10",
]
GENERATOR_LEGAL_ENTRIES = {
    "META-INF/third-party/THIRD-PARTY.txt",
    "META-INF/third-party/jetbrains-annotations/LICENSE.txt",
    "META-INF/third-party/kotlin-stdlib/LICENSE.txt",
    "META-INF/third-party/kotlin-stdlib/NOTICE.txt",
    "META-INF/third-party/protobuf-java/LICENSE.txt",
}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def artifact_path(repository: pathlib.Path, module: str, suffix: str) -> pathlib.Path:
    return repository / GROUP.replace(".", "/") / module / VERSION / f"{module}-{VERSION}{suffix}"


def resolve_version(repository: pathlib.Path) -> str:
    if len(sys.argv) >= 3 and sys.argv[2].strip():
        return sys.argv[2].strip()
    env_version = (os.environ.get("TREVRPC_VERSION") or os.environ.get("TREVRPC_KOTLIN_VERSION") or "").strip()
    if env_version:
        return env_version
    group_directory = repository / GROUP.replace(".", "/")
    if group_directory.is_dir():
        versions: set[str] = set()
        for module in MODULE_TARGETS:
            module_dir = group_directory / module
            if module_dir.is_dir():
                versions.update(p.name for p in module_dir.iterdir() if p.is_dir())
        if len(versions) == 1:
            return next(iter(versions))
    return VERSION


def verify_checksums(path: pathlib.Path) -> None:
    for extension in (".md5", ".sha1", ".sha256", ".sha512"):
        checksum = pathlib.Path(f"{path}{extension}")
        require(checksum.is_file(), f"missing checksum {checksum}")
        require(checksum.stat().st_size > 0, f"empty checksum {checksum}")


def verify_archive(
    path: pathlib.Path,
    expected_major: int | None = None,
    maximum_major: int | None = None,
    required_suffixes: tuple[str, ...] = (),
) -> None:
    require(path.is_file(), f"missing archive {path}")
    require(path.stat().st_size > 0, f"empty archive {path}")
    verify_checksums(path)
    with zipfile.ZipFile(path) as archive:
        names = archive.namelist()
        require(names, f"empty archive {path}")
        require("META-INF/LICENSE" in names, f"missing MIT license in {path}")
        require(len(names) == len(set(names)), f"duplicate archive entries in {path}")
        if required_suffixes:
            require(
                any(name.endswith(required_suffixes) for name in names),
                f"{path} has no files ending in {required_suffixes}",
            )
        for info in archive.infolist():
            require(info.date_time == (1980, 2, 1, 0, 0, 0), f"non-reproducible timestamp in {path}: {info.filename}")
        if expected_major is not None or maximum_major is not None:
            classes = [name for name in names if name.endswith(".class")]
            require(classes, f"no class files in {path}")
            for name in classes:
                data = archive.read(name)
                require(data[:4] == b"\xca\xfe\xba\xbe", f"invalid class file {path}!{name}")
                major = struct.unpack(">H", data[6:8])[0]
                if expected_major is not None:
                    require(major == expected_major, f"{path}!{name} has class major {major}, expected {expected_major}")
                if maximum_major is not None:
                    require(major <= maximum_major, f"{path}!{name} has class major {major}, maximum {maximum_major}")


def verify_class_major(
    archive: zipfile.ZipFile,
    path: pathlib.Path,
    name: str,
    expected_major: int,
) -> None:
    require(name in archive.namelist(), f"missing class {path}!{name}")
    data = archive.read(name)
    require(data[:4] == b"\xca\xfe\xba\xbe", f"invalid class file {path}!{name}")
    major = struct.unpack(">H", data[6:8])[0]
    require(major == expected_major, f"{path}!{name} has class major {major}, expected {expected_major}")


def dependency_rows(root: ET.Element) -> list[tuple[str, str, str | None, str, str | None]]:
    rows = []
    for dependency in root.findall("m:dependencies/m:dependency", POM_NAMESPACE):
        rows.append(
            (
                dependency.findtext("m:groupId", namespaces=POM_NAMESPACE) or "",
                dependency.findtext("m:artifactId", namespaces=POM_NAMESPACE) or "",
                dependency.findtext("m:version", namespaces=POM_NAMESPACE),
                dependency.findtext("m:scope", default="compile", namespaces=POM_NAMESPACE),
                dependency.findtext("m:classifier", namespaces=POM_NAMESPACE),
            )
        )
    return rows


def verify_pom(path: pathlib.Path, module: str) -> None:
    require(path.is_file(), f"missing POM {path}")
    verify_checksums(path)
    root = ET.parse(path).getroot()
    require(root.findtext("m:groupId", namespaces=POM_NAMESPACE) == GROUP, f"wrong group in {path}")
    require(root.findtext("m:artifactId", namespaces=POM_NAMESPACE) == module, f"wrong artifact in {path}")
    require(root.findtext("m:version", namespaces=POM_NAMESPACE) == VERSION, f"wrong version in {path}")
    require(root.findtext("m:url", namespaces=POM_NAMESPACE) == "https://trev.zip/llc/TrevRPC", f"wrong URL in {path}")
    require(root.findtext("m:inceptionYear", namespaces=POM_NAMESPACE) == "2026", f"wrong inception year in {path}")
    require(root.findtext("m:licenses/m:license/m:name", namespaces=POM_NAMESPACE) == "MIT License", f"missing MIT metadata in {path}")
    require(root.findtext("m:developers/m:developer/m:id", namespaces=POM_NAMESPACE) == "trev", f"missing developer metadata in {path}")
    require(root.findtext("m:scm/m:connection", namespaces=POM_NAMESPACE) == "scm:git:https://trev.zip/llc/TrevRPC.git", f"wrong SCM metadata in {path}")

    rows = dependency_rows(root)
    if module == "core":
        require(("org.jetbrains.kotlinx", "kotlinx-coroutines-core-jvm", "1.11.0", "compile", None) in rows, "core coroutines must be compile scope")
        require(("com.google.protobuf", "protobuf-java", "4.35.1", "compile", None) in rows, "core protobuf must be compile scope")
    elif module == "transport-netty":
        require((GROUP, "core", VERSION, "compile", None) in rows, "Netty core must be compile scope")
        require(any(row[1] == "netty-codec-classes-quic" and row[3] == "compile" for row in rows), "Netty QUIC classes must be compile scope")
        require(any(row[1] == "netty-codec-http3" and row[3] == "compile" for row in rows), "Netty HTTP/3 must be compile scope")
        natives = [row for row in rows if row[0:2] == ("io.netty", "netty-codec-native-quic")]
        require({row[4] for row in natives} == NATIVE_CLASSIFIERS, "Netty POM must contain exactly five native classifiers")
        require(all(row[3] == "runtime" for row in natives), "Netty native classifiers must be runtime scope")
        require(all(row[4] is not None for row in natives), "Netty POM contains an unclassified native dependency")
    elif module == "transport-cronet":
        require((GROUP, "core", VERSION, "compile", None) in rows, "Cronet core must be compile scope")
        require(not any(row[0] == "org.chromium.net" for row in rows), "Cronet POM must not expose bundled API classes or choose a provider")
        description = root.findtext("m:description", namespaces=POM_NAMESPACE) or ""
        require("JVM 17" in description, f"Cronet POM has an untruthful platform description: {description!r}")
        license_names = {
            license_element.findtext("m:name", namespaces=POM_NAMESPACE)
            for license_element in root.findall("m:licenses/m:license", POM_NAMESPACE)
        }
        require("Chromium and built-in dependencies" in license_names, "Cronet POM omits bundled class licensing")
    else:
        require(any(row[1] == "protobuf-java" and row[3] == "runtime" for row in rows), "generator protobuf must be runtime scope")


def verify_module_metadata(path: pathlib.Path, module: str) -> None:
    require(path.is_file(), f"missing Gradle module metadata {path}")
    verify_checksums(path)
    data = json.loads(path.read_text())
    require(data["createdBy"]["gradle"]["version"] == "9.5.1", f"wrong Gradle version in {path}")
    library_variants = [variant for variant in data["variants"] if variant["name"] in {"apiElements", "runtimeElements"}]
    require(len(library_variants) == 2, f"missing library variants in {path}")
    require(all(variant["attributes"]["org.gradle.jvm.version"] == TARGET_JVM[module] for variant in library_variants), f"wrong JVM attributes in {path}")

    if module == "transport-netty":
        runtime = next(variant for variant in library_variants if variant["name"] == "runtimeElements")
        selectors = {
            dependency.get("thirdPartyCompatibility", {}).get("artifactSelector", {}).get("classifier")
            for dependency in runtime["dependencies"]
            if dependency["module"] == "netty-codec-native-quic"
        }
        require(selectors == NATIVE_CLASSIFIERS, "Gradle metadata must select all five Netty native classifiers")
        require(None not in selectors, "Gradle metadata contains an unclassified Netty native")
    elif module == "transport-cronet":
        require(
            not any(
                dependency["group"] == "org.chromium.net"
                for variant in library_variants
                for dependency in variant.get("dependencies", [])
            ),
            "Cronet Gradle metadata must not expose bundled API classes or choose a provider",
        )


def verify_generator_legal_metadata(archive: zipfile.ZipFile, path: pathlib.Path) -> None:
    names = set(archive.namelist())
    require(GENERATOR_LEGAL_ENTRIES <= names, f"missing generator third-party legal metadata in {path}")
    for name in GENERATOR_LEGAL_ENTRIES:
        require(archive.getinfo(name).file_size > 0, f"empty generator legal metadata {path}!{name}")

    inventory = archive.read("META-INF/third-party/THIRD-PARTY.txt").decode("utf-8")
    coordinates = [line for line in inventory.splitlines() if line.count(":") == 2 and not line.startswith(" ")]
    require(coordinates == GENERATOR_THIRD_PARTY_COORDINATES, f"wrong or non-deterministic third-party inventory in {path}")
    require("Apache License" in archive.read("META-INF/third-party/kotlin-stdlib/LICENSE.txt").decode("utf-8"), f"wrong Kotlin license in {path}")
    require("NOTICE file" in archive.read("META-INF/third-party/kotlin-stdlib/NOTICE.txt").decode("utf-8"), f"wrong Kotlin notice in {path}")
    require("Copyright 2008 Google Inc." in archive.read("META-INF/third-party/protobuf-java/LICENSE.txt").decode("utf-8"), f"wrong protobuf license in {path}")


def main() -> None:
    require(len(sys.argv) in (2, 3), "usage: verify_staged_repository.py REPOSITORY [VERSION]")
    repository = pathlib.Path(sys.argv[1]).resolve()
    global VERSION
    VERSION = resolve_version(repository)
    group_directory = repository / GROUP.replace(".", "/")
    require(group_directory.is_dir(), f"missing group directory {group_directory}")
    modules = {path.name for path in group_directory.iterdir() if path.is_dir()}
    require(modules == set(MODULE_TARGETS), f"unexpected staged coordinates: {sorted(modules)}")

    for module, class_major in MODULE_TARGETS.items():
        base = artifact_path(repository, module, "")
        main_jar = pathlib.Path(f"{base}.jar")
        sources_jar = artifact_path(repository, module, "-sources.jar")
        javadoc_jar = artifact_path(repository, module, "-javadoc.jar")
        pom = pathlib.Path(f"{base}.pom")
        module_metadata = pathlib.Path(f"{base}.module")

        verify_archive(main_jar, class_major)
        if module == "transport-cronet":
            with zipfile.ZipFile(main_jar) as archive:
                names = set(archive.namelist())
                require("org/chromium/net/CronetEngine.class" in names, "Cronet JAR does not bundle its public API classes")
                require("META-INF/LICENSE.chromium-cronet" in names, "Cronet JAR omits bundled class licensing")
        verify_archive(sources_jar, required_suffixes=(".kt", ".java"))
        verify_archive(javadoc_jar, required_suffixes=(".html",))
        verify_pom(pom, module)
        verify_module_metadata(module_metadata, module)

        version_directory = main_jar.parent
        allowed_artifacts = {main_jar.name, sources_jar.name, javadoc_jar.name, pom.name, module_metadata.name}
        if module == "protoc-gen-trevrpc-kotlin":
            with zipfile.ZipFile(main_jar) as archive:
                manifest = archive.read("META-INF/MANIFEST.MF").decode()
                require("Main-Class: zip.trev.trevrpc.generator.MainKt" in manifest, "thin generator has wrong Main-Class")
            executable = artifact_path(repository, module, "-jdk21.jar")
            verify_archive(executable, maximum_major=65)
            with zipfile.ZipFile(executable) as archive:
                manifest = archive.read("META-INF/MANIFEST.MF").decode()
                require("Main-Class: zip.trev.trevrpc.generator.MainKt" in manifest, "executable generator has wrong Main-Class")
                verify_class_major(archive, executable, "zip/trev/trevrpc/generator/MainKt.class", 65)
                verify_generator_legal_metadata(archive, executable)
            process = subprocess.run(["java", "-jar", executable], input=b"", stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
            require(process.returncode == 0, f"executable generator failed: {process.stderr.decode(errors='replace')}")
            allowed_artifacts.add(executable.name)
        else:
            require(not list(version_directory.glob(f"{module}-{VERSION}-jdk21.*")), f"unexpected jdk21 classifier for {module}")

        published_all = {
            path.name
            for path in version_directory.iterdir()
            if path.is_file() and not any(path.name.endswith(suffix) for suffix in (".md5", ".sha1", ".sha256", ".sha512"))
        }
        asc_files = {name for name in published_all if name.endswith(".asc")}
        published = published_all - asc_files
        require(published == allowed_artifacts, f"unexpected artifacts for {module}: {sorted(published - allowed_artifacts)}")
        for asc_name in asc_files:
            asc_path = version_directory / asc_name
            verify_checksums(asc_path)
            require(asc_path.stat().st_size > 0, f"empty signature {asc_path}")
            require(asc_path.read_bytes().startswith(b"-----BEGIN PGP"), f"invalid PGP signature {asc_path}")

    print("staged Maven repository verified")


if __name__ == "__main__":
    main()
