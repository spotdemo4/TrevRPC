import com.google.protobuf.gradle.id
import org.gradle.api.DefaultTask
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.TaskAction

abstract class VerifyCanonicalBenchmarkProto : DefaultTask() {
    @get:InputFile
    abstract val packagedProto: RegularFileProperty

    @get:InputFile
    @get:Optional
    abstract val canonicalProto: RegularFileProperty

    @TaskAction
    fun verify() {
        if (!canonicalProto.isPresent) return
        check(
            canonicalProto
                .get()
                .asFile
                .readBytes()
                .contentEquals(packagedProto.get().asFile.readBytes()),
        ) {
            "bench-peer/src/main/proto/benchmark.proto differs from ../bench/proto/benchmark.proto"
        }
    }
}

plugins {
    application
    kotlin("jvm")
    id("com.google.protobuf") version "0.9.5"
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
    sourceSets.main {
        kotlin.srcDir(layout.buildDirectory.dir("generated/sources/proto/main/trevrpc-kotlin"))
    }
}

val canonicalBenchmarkProto = rootProject.layout.projectDirectory.file("../bench/proto/benchmark.proto")
val packagedBenchmarkProto = layout.projectDirectory.file("src/main/proto/benchmark.proto")
val benchmarkProtoSource =
    if (canonicalBenchmarkProto.asFile.isFile) {
        canonicalBenchmarkProto.asFile.parentFile
    } else {
        packagedBenchmarkProto.asFile.parentFile
    }

sourceSets {
    main {
        proto.setSrcDirs(listOf(benchmarkProtoSource))
    }
}

application {
    mainClass.set("zip.trev.trevrpc.bench.PeerKt")
    applicationName = "trevrpc-bench-peer-kotlin"
    applicationDefaultJvmArgs = listOf("--enable-native-access=ALL-UNNAMED")
}

dependencies {
    implementation(project(":core"))
    implementation(project(":transport-netty"))
    implementation("com.google.protobuf:protobuf-java:4.35.1")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")

    runtimeOnly("io.netty:netty-codec-native-quic:4.2.16.Final:linux-x86_64")

    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation(platform("io.netty:netty-bom:4.2.16.Final"))
    testImplementation("io.netty:netty-handler")
    testImplementation("org.bouncycastle:bcpkix-jdk18on:1.84")
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
    testRuntimeOnly("io.netty:netty-codec-native-quic:4.2.16.Final:linux-x86_64")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

val trevrpcPlugin =
    project(":protoc-gen-trevrpc-kotlin")
        .layout.buildDirectory
        .file("install/protoc-gen-trevrpc-kotlin/bin/protoc-gen-trevrpc-kotlin")

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.35.1"
    }
    plugins {
        id("trevrpc-kotlin") {
            path = trevrpcPlugin.get().asFile.absolutePath
        }
    }
    generateProtoTasks {
        all().configureEach {
            dependsOn(":protoc-gen-trevrpc-kotlin:installDist")
            plugins {
                id("trevrpc-kotlin")
            }
        }
    }
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}

val verifyCanonicalBenchmarkProto =
    tasks.register<VerifyCanonicalBenchmarkProto>("verifyCanonicalBenchmarkProto") {
        packagedProto.set(packagedBenchmarkProto)
        if (canonicalBenchmarkProto.asFile.isFile) canonicalProto.set(canonicalBenchmarkProto)
    }

tasks.named("check") {
    dependsOn(verifyCanonicalBenchmarkProto)
}
