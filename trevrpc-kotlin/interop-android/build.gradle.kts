import org.gradle.api.DefaultTask
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import org.gradle.process.ExecOperations
import javax.inject.Inject

/**
 * Generates the Echo and UnreliableEcho clients from the shared interop schemas with `protoc` from
 * `PATH`. The schemas are the single source of truth for every runtime, so this module references
 * them instead of keeping copies. The task is not cacheable because its output depends on the
 * `protoc` binary.
 *
 * `unreliable.proto` imports `trevrpc/options.proto`, which lives in its own include root, so that
 * file is compiled too: the generated Java for `unreliable.proto` refers to its descriptor class.
 */
abstract class GenerateInteropClient
    @Inject
    constructor(
        private val exec: ExecOperations,
    ) : DefaultTask() {
        /** Directory holding `interop.proto` and `unreliable.proto`. */
        @get:InputDirectory
        @get:PathSensitive(PathSensitivity.RELATIVE)
        abstract val schemaRoot: DirectoryProperty

        /** Include root holding `trevrpc/options.proto`. */
        @get:InputDirectory
        @get:PathSensitive(PathSensitivity.RELATIVE)
        abstract val optionsRoot: DirectoryProperty

        /** Schema paths relative to [schemaRoot]; the options schema is added automatically. */
        @get:Input
        abstract val schemas: ListProperty<String>

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
            val plugin = generatorInstall.get().file("bin/protoc-gen-trevrpc-kotlin").asFile
            exec.exec {
                commandLine(
                    "protoc",
                    "--proto_path=${schemaRoot.get().asFile}",
                    "--proto_path=${optionsRoot.get().asFile}",
                    "--java_out=${javaOutput.get().asFile}",
                    "--plugin=protoc-gen-trevrpc-kotlin=$plugin",
                    "--trevrpc-kotlin_out=${kotlinOutput.get().asFile}",
                    *schemas.get().toTypedArray(),
                    "trevrpc/options.proto",
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
        minSdk = 24
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
        schemaRoot.set(rootProject.layout.projectDirectory.dir("../interop/proto"))
        optionsRoot.set(rootProject.layout.projectDirectory.dir("../proto"))
        schemas.set(listOf("interop.proto", "unreliable.proto"))
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
    implementation(libs.cronet.bundled)

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
