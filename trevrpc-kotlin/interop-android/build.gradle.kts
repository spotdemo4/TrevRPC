import org.gradle.api.DefaultTask
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import org.gradle.process.ExecOperations
import javax.inject.Inject

/**
 * Generates the Echo client from the shared interop schema with `protoc` from `PATH`. The schema is
 * the single source of truth for every runtime, so this module references it instead of keeping a
 * copy. The task is not cacheable because its output depends on the `protoc` binary.
 */
abstract class GenerateInteropClient
    @Inject
    constructor(
        private val exec: ExecOperations,
    ) : DefaultTask() {
        @get:InputFile
        @get:PathSensitive(PathSensitivity.NAME_ONLY)
        abstract val schema: RegularFileProperty

        @get:InputDirectory
        @get:PathSensitive(PathSensitivity.RELATIVE)
        abstract val generatorInstall: DirectoryProperty

        @get:OutputDirectory
        abstract val javaOutput: DirectoryProperty

        @get:OutputDirectory
        abstract val kotlinOutput: DirectoryProperty

        @TaskAction
        fun generate() {
            for (output in listOf(javaOutput, kotlinOutput)) {
                val directory = output.get().asFile
                directory.deleteRecursively()
                directory.mkdirs()
            }
            val schemaFile = schema.get().asFile
            val plugin = generatorInstall.get().file("bin/protoc-gen-trevrpc-kotlin").asFile
            exec.exec {
                commandLine(
                    "protoc",
                    "--proto_path=${schemaFile.parentFile}",
                    "--java_out=${javaOutput.get().asFile}",
                    "--plugin=protoc-gen-trevrpc-kotlin=$plugin",
                    "--trevrpc-kotlin_out=${kotlinOutput.get().asFile}",
                    schemaFile.name,
                )
            }
        }
    }

plugins {
    alias(libs.plugins.android.application)
}

android {
    namespace = "zip.trev.trevrpc.interop.android"
    compileSdk = 36
    buildToolsVersion = "37.0.0"

    defaultConfig {
        applicationId = "zip.trev.trevrpc.interop.android"
        minSdk = 23
        targetSdk = 36
        versionCode = 1
        versionName = "1"
        testInstrumentationRunner = "androidx.test.runner.AndroidJUnitRunner"
    }

    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

val generateInteropClient =
    tasks.register<GenerateInteropClient>("generateInteropClient") {
        schema.set(rootProject.layout.projectDirectory.file("../interop/proto/interop.proto"))
        generatorInstall.set(project(":protoc-gen-trevrpc-kotlin").layout.buildDirectory.dir("install/protoc-gen-trevrpc-kotlin"))
        javaOutput.set(layout.buildDirectory.dir("generated/interop/java"))
        kotlinOutput.set(layout.buildDirectory.dir("generated/interop/kotlin"))
        dependsOn(":protoc-gen-trevrpc-kotlin:installDist")
    }

androidComponents.onVariants { variant ->
    variant.sources.java?.addGeneratedSourceDirectory(generateInteropClient, GenerateInteropClient::javaOutput)
    variant.sources.kotlin?.addGeneratedSourceDirectory(generateInteropClient, GenerateInteropClient::kotlinOutput)
}

dependencies {
    implementation(project(":transport-cronet"))
    implementation(libs.cronet.embedded) {
        exclude(group = "org.chromium.net", module = "cronet-api")
        exclude(group = "com.google.protobuf", module = "protobuf-javalite")
    }

    androidTestImplementation(libs.android.test.junit)
    androidTestImplementation(libs.android.test.runner)
}

tasks.register("resolveAndLockAll") {
    dependsOn("dependencies")
    doFirst {
        require(gradle.startParameter.isWriteDependencyLocks) {
            "$path must be run with --write-locks"
        }
    }
}
