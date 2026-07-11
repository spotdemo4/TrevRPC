import com.google.protobuf.gradle.id
import org.gradle.api.DefaultTask
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.TaskAction

abstract class VerifyGeneratedTrevrpc : DefaultTask() {
    @get:InputFile
    abstract val generatedBinding: RegularFileProperty

    @get:InputFile
    abstract val checkedInBinding: RegularFileProperty

    @TaskAction
    fun verify() {
        check(
            checkedInBinding
                .get()
                .asFile
                .readBytes()
                .contentEquals(generatedBinding.get().asFile.readBytes()),
        ) {
            "checked-in TrevRPC binding is stale; run :examples:syncGeneratedTrevrpc"
        }
    }
}

plugins {
    application
    kotlin("jvm")
    id("com.google.protobuf") version "0.9.5"
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(21))
}

kotlin {
    jvmToolchain(21)
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
        allWarningsAsErrors.set(true)
    }
    sourceSets.main {
        kotlin.srcDir(layout.buildDirectory.dir("generated/sources/proto/main/trevrpc-kotlin"))
    }
}

application {
    mainClass.set("zip.trev.trevrpc.examples.XRuntimeKt")
    applicationName = "trevrpc-xruntime-kotlin"
}

dependencies {
    implementation(project(":core"))
    implementation(project(":transport-netty"))
    implementation("com.google.protobuf:protobuf-java:4.35.1")
    implementation("org.bouncycastle:bcpkix-jdk18on:1.84")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")

    runtimeOnly("io.netty:netty-codec-native-quic:4.2.16.Final:linux-x86_64")

    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
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

val generatedBindingFile =
    layout.buildDirectory.file("generated/sources/proto/main/trevrpc-kotlin/greeter.trevrpc.kt")
val checkedInBindingFile = layout.projectDirectory.file("generated/trevrpc/greeter.trevrpc.kt")

tasks.register<Sync>("syncGeneratedTrevrpc") {
    dependsOn(tasks.named("generateProto"))
    from(generatedBindingFile)
    into(checkedInBindingFile.asFile.parentFile)
}

val verifyGeneratedTrevrpc =
    tasks.register<VerifyGeneratedTrevrpc>("verifyGeneratedTrevrpc") {
        dependsOn(tasks.named("generateProto"))
        generatedBinding.set(generatedBindingFile)
        checkedInBinding.set(checkedInBindingFile)
    }

tasks.named("check") {
    dependsOn(verifyGeneratedTrevrpc)
}

tasks.test {
    useJUnitPlatform()
}
