plugins {
    alias(libs.plugins.android.application)
}

android {
    namespace = "zip.trev.trevrpc.bench.cronet"
    compileSdk = 36
    buildToolsVersion = "37.0.0"

    defaultConfig {
        applicationId = "zip.trev.trevrpc.bench.cronet"
        minSdk = 23
        targetSdk = 36
        versionCode = 1
        versionName = "1"
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

dependencies {
    implementation(project(":benchmark-support"))
    implementation(project(":transport-cronet"))
    implementation(libs.cronet.embedded) {
        exclude(group = "org.chromium.net", module = "cronet-api")
        exclude(group = "com.google.protobuf", module = "protobuf-javalite")
    }

    androidTestImplementation(libs.android.test.junit)
    androidTestImplementation(libs.android.test.runner)
}

tasks.register("resolveAndLockAll") {
    dependsOn("dependencies")
    doFirst {
        require(gradle.startParameter.isWriteDependencyLocks) {
            "$path must be run with --write-locks"
        }
    }
}
