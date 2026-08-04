plugins {
    kotlin("jvm") version "2.4.10"
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(25))
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

kotlin {
    jvmToolchain(25)
    compilerOptions.jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
    sourceSets.main {
        kotlin.srcDir("../sources")
    }
}

dependencies {
    implementation("zip.trev.trevrpc:transport-cronet:0.1.0")
}
