pluginManagement {
    repositories {
        google()
        mavenCentral()
        gradlePluginPortal()
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
    "transport-cronet",
    "protoc-gen-trevrpc-kotlin",
)

// The Android interop app needs the Android SDK, so it is opt-in.
if (providers.gradleProperty("trevrpcAndroidInterop").map(String::toBoolean).getOrElse(false)) {
    include("interop-android")
}
