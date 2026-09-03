pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
    }
    resolutionStrategy {
        eachPlugin {
            if (requested.id.id == "com.google.protobuf") {
                useModule("com.google.protobuf:protobuf-gradle-plugin:${requested.version}")
            }
        }
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        exclusiveContent {
            forRepository { google() }
            filter {
                includeGroupByRegex("androidx\\..*")
                includeGroupByRegex("com\\.android(\\..*)?")
                includeGroup("org.chromium.net")
            }
        }
        mavenCentral()
    }
}

rootProject.name = "trevrpc-kotlin"

include(
    "core",
    "transport-netty",
    "transport-cronet",
    "protoc-gen-trevrpc-kotlin",
    "examples",
    "benchmark-support",
    "bench-peer-netty",
    "conformance-peer",
)

if (providers.gradleProperty("trevrpcCronetBenchPeer").map(String::toBoolean).getOrElse(false)) {
    include("bench-peer-cronet")
}
