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

application {
    mainClass.set("zip.trev.trevrpc.examples.XRuntimeKt")
    applicationName = "trevrpc-xruntime-kotlin"
    applicationDefaultJvmArgs = listOf("--enable-native-access=ALL-UNNAMED")
}

dependencies {
    implementation(project(":core"))
    implementation(project(":transport-netty"))
    implementation(libs.protobuf.java)
    implementation(libs.bouncycastle.bcpkix)
    implementation(libs.coroutines.core)

    runtimeOnly("io.netty:netty-codec-native-quic:${libs.versions.netty.get()}:linux-x86_64")

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(libs.coroutines.test)
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
    jvmArgs("--enable-native-access=ALL-UNNAMED")
}
