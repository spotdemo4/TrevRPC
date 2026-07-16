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

dependencies {
    implementation(project(":core"))
    implementation(libs.protobuf.java)
    implementation(libs.coroutines.core)

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
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
}
