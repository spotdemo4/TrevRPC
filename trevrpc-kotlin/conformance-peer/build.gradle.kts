plugins {
    application
    kotlin("jvm")
    alias(libs.plugins.protobuf)
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(25))
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

kotlin {
    jvmToolchain(25)
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
        allWarningsAsErrors.set(true)
    }
}

application {
    mainClass.set("zip.trev.trevrpc.conformance.MainKt")
    applicationName = "trevrpc-conformance-kotlin"
    applicationDefaultJvmArgs = listOf("-Xms16m", "-Xmx256m")
}

dependencies {
    implementation(project(":core"))
    implementation(libs.coroutines.core)
    implementation(libs.protobuf.java)

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(libs.coroutines.test)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

val configuredProtoc = providers.gradleProperty("trevrpcProtocPath")

protobuf {
    protoc {
        if (configuredProtoc.isPresent) {
            val protocFile = rootProject.file(configuredProtoc.get())
            require(protocFile.isFile && protocFile.canExecute()) {
                "trevrpcProtocPath must name an executable protoc binary: $protocFile"
            }
            path = protocFile.absolutePath
        } else {
            artifact =
                libs.protobuf.protoc
                    .get()
                    .toString()
        }
    }
}

tasks.test {
    dependsOn(tasks.installDist)
    useJUnitPlatform()
    systemProperty(
        "trevrpc.kotlin.conformance.peer",
        layout.buildDirectory
            .file("install/trevrpc-conformance-kotlin/bin/trevrpc-conformance-kotlin")
            .get()
            .asFile,
    )
}
