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

val trevrpcVersion =
    providers
        .gradleProperty("trevrpcVersion")
        .orElse(providers.environmentVariable("TREVRPC_VERSION"))
        .orElse(providers.gradleProperty("trevrpc.version"))
        .orElse(
            providers
                .fileContents(layout.projectDirectory.file("../../../build.gradle.kts"))
                .asText
                .map { text ->
                    Regex("""version = "([0-9]+\.[0-9]+\.[0-9]+)"""")
                        .find(text)
                        ?.groupValues
                        ?.get(1) ?: ""
                }.filter { it.isNotEmpty() },
        ).getOrElse("0.1.1")

dependencies {
    implementation("zip.trev.trevrpc:transport-cronet:$trevrpcVersion")
}
