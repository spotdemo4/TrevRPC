plugins {
    kotlin("jvm")
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(25))
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

kotlin {
    jvmToolchain(25)
    compilerOptions.jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
    sourceSets.main {
        kotlin.srcDir("../../sources/netty")
    }
}

val trevrpcVersion =
    providers
        .gradleProperty("trevrpcVersion")
        .orElse(providers.environmentVariable("TREVRPC_VERSION"))
        .orElse(providers.gradleProperty("trevrpc.version"))
        .getOrElse("0.1.1")

dependencies {
    implementation("zip.trev.trevrpc:transport-netty:$trevrpcVersion")
}
