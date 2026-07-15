import com.google.protobuf.gradle.id
import org.gradle.api.DefaultTask
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.Optional
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption

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

abstract class InstallGrpcJavaPlugin : DefaultTask() {
    @get:InputFile
    abstract val source: RegularFileProperty

    @get:OutputFile
    abstract val destination: RegularFileProperty

    @get:OutputFile
    abstract val binary: RegularFileProperty

    @TaskAction
    fun install() {
        val wrapper = destination.get().asFile
        val executable = binary.get().asFile
        check(wrapper.parentFile.mkdirs() || wrapper.parentFile.isDirectory) {
            "failed to create gRPC plugin directory"
        }
        Files.copy(source.get().asFile.toPath(), executable.toPath(), StandardCopyOption.REPLACE_EXISTING)
        check(executable.setExecutable(true)) { "failed to make protoc-gen-grpc-java executable" }

        val mappedLibraries =
            Files
                .readAllLines(Path.of("/proc/self/maps"))
                .asSequence()
                .map { Path.of(it.substringAfterLast(' ')) }
                .filter { it.isAbsolute && it.parent != null }
                .toList()
        val libc = mappedLibraries.first { it.fileName.toString() == "libc.so.6" }
        val loader = libc.parent.resolve("ld-linux-x86-64.so.2")
        check(Files.isRegularFile(loader)) { "failed to locate the glibc dynamic loader" }
        val libraryPath = mappedLibraries.map { it.parent }.distinct().joinToString(":")
        val shell = System.getenv("SHELL") ?: "/bin/sh"
        wrapper.writeText(
            """
            #!$shell
            exec "$loader" --library-path "$libraryPath" "$executable" "${'$'}@"
            """.trimIndent() + "\n",
        )
        check(wrapper.setExecutable(true)) { "failed to make the protoc-gen-grpc-java wrapper executable" }
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
        kotlin.srcDir(layout.buildDirectory.dir("generated/sources/proto/main/grpckt"))
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
    implementation(libs.grpc.kotlin.stub)
    implementation(libs.grpc.netty.shaded)
    implementation(libs.grpc.protobuf)
    implementation(libs.grpc.stub)

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

val grpcJavaCodegen =
    configurations.create("grpcJavaCodegen") {
        isCanBeConsumed = false
        isTransitive = false
    }
dependencies.add(grpcJavaCodegen.name, "${libs.grpc.protoc.java.get()}:linux-x86_64@exe")
val grpcJavaPlugin = layout.buildDirectory.file("grpc-tools/protoc-gen-grpc-java")
val grpcJavaPluginBinary = layout.buildDirectory.file("grpc-tools/protoc-gen-grpc-java.bin")
val installGrpcJavaPlugin =
    tasks.register<InstallGrpcJavaPlugin>("installGrpcJavaPlugin") {
        source.fileProvider(grpcJavaCodegen.elements.map { it.single().asFile })
        destination.set(grpcJavaPlugin)
        binary.set(grpcJavaPluginBinary)
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
        id("grpc") {
            path = grpcJavaPlugin.get().asFile.absolutePath
        }
        id("grpckt") {
            artifact = "${libs.grpc.protoc.kotlin.get()}:jdk8@jar"
        }
        id("trevrpc-kotlin") {
            path = trevrpcPlugin.get().asFile.absolutePath
        }
    }
    generateProtoTasks {
        all().configureEach {
            dependsOn(installGrpcJavaPlugin, ":protoc-gen-trevrpc-kotlin:installDist")
            plugins {
                id("grpc")
                id("grpckt")
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
