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
    applicationDefaultJvmArgs =
        listOf(
            "--enable-native-access=ALL-UNNAMED",
            "-Xms32m",
            "-Xmx512m",
        )
}

dependencies {
    implementation(project(":core"))
    implementation(project(":transport-netty"))
    implementation(libs.protobuf.java)
    implementation(libs.coroutines.core)

    runtimeOnly("io.netty:netty-codec-native-quic:${libs.versions.netty.get()}:linux-x86_64")

    testImplementation(platform(libs.junit.bom))
    testImplementation(platform(libs.netty.bom))
    testImplementation("io.netty:netty-handler")
    testImplementation(libs.bouncycastle.bcpkix)
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(libs.coroutines.test)
    testRuntimeOnly("io.netty:netty-codec-native-quic:${libs.versions.netty.get()}:linux-x86_64")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

val trevrpcPlugin =
    project(":protoc-gen-trevrpc-kotlin")
        .layout.buildDirectory
        .file("install/protoc-gen-trevrpc-kotlin/bin/protoc-gen-trevrpc-kotlin")

protobuf {
    protoc {
        artifact =
            libs.protobuf.protoc
                .get()
                .toString()
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
