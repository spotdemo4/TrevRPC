import com.android.build.api.artifact.SingleArtifact
import org.gradle.api.DefaultTask
import org.gradle.api.file.DirectoryProperty
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.ListProperty
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.InputDirectory
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputDirectory
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import org.gradle.process.ExecOperations
import javax.inject.Inject

/**
 * Generates the Echo, UnreliableEcho, CompressedEcho and ValidatedEcho clients from the shared interop schemas with
 * `protoc` from `PATH`, and the `buf.validate` messages of ValidatedEcho's violations from the copy of
 * `buf/validate/validate.proto` beside them. The schemas are the single source of truth for every runtime, so this module references
 * them instead of keeping copies. The task is not cacheable because its output depends on the
 * `protoc` binary.
 *
 * The Java messages are generated for protobuf-javalite, the runtime Android apps use. Lite code does
 * not refer to the files a schema imports only for their options, so `trevrpc/options.proto`, which
 * `unreliable.proto` and `compressed.proto` import from its own include root, is not compiled.
 */
abstract class GenerateInteropClient
    @Inject
    constructor(
        private val exec: ExecOperations,
    ) : DefaultTask() {
        /** Directory holding `interop.proto`, `unreliable.proto`, `compressed.proto` and `validated.proto`. */
        @get:InputDirectory
        @get:PathSensitive(PathSensitivity.RELATIVE)
        abstract val schemaRoot: DirectoryProperty

        /** Include root holding `trevrpc/options.proto`, which the schemas import. */
        @get:InputDirectory
        @get:PathSensitive(PathSensitivity.RELATIVE)
        abstract val optionsRoot: DirectoryProperty

        /** Schema paths relative to [schemaRoot]. */
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
                    "--java_out=lite:${javaOutput.get().asFile}",
                    "--plugin=protoc-gen-trevrpc-kotlin=$plugin",
                    "--trevrpc-kotlin_out=${kotlinOutput.get().asFile}",
                    *schemas.get().toTypedArray(),
                )
            }
        }
    }

/**
 * Checks R8's output for a minified build that includes zstd-jni. zstd-jni's native code finds its
 * classes and fields by name, such as the `nativePtr` of the dictionaries, so every zstd-jni class R8
 * keeps must keep its name and all of its fields under their own names. The rules core ships do that;
 * without them R8 removes `ZstdDictCompress.nativePtr`.
 */
abstract class VerifyZstdShrinking : DefaultTask() {
    /** R8's `mapping.txt`, which names each class and member it keeps, and any new name. */
    @get:InputFile
    @get:PathSensitive(PathSensitivity.NONE)
    abstract val mappingFile: RegularFileProperty

    /** R8's `usage.txt` beside it, which names each class and member it removes. */
    @get:InputFile
    @get:PathSensitive(PathSensitivity.NONE)
    abstract val usageFile: RegularFileProperty

    /** R8's `seeds.txt` beside it, which names each class and member a keep rule kept. */
    @get:InputFile
    @get:PathSensitive(PathSensitivity.NONE)
    abstract val seedsFile: RegularFileProperty

    @TaskAction
    fun verify() {
        var keptClass: String? = null
        for (line in mappingFile.get().asFile.readLines()) {
            if (line.startsWith("#") || line.isBlank()) continue
            if (!line.startsWith(" ")) {
                val (original, renamed) = line.removeSuffix(":").split(" -> ")
                keptClass = original.takeIf { it.startsWith(ZSTD_PACKAGE) }
                keptClass?.let { check(renamed == it) { "R8 renamed $it to $renamed" } }
            } else if (keptClass != null && "(" !in line) {
                // A field: "    type name -> newName".
                val (declaration, renamed) = line.trim().split(" -> ")
                val name = declaration.substringAfterLast(' ')
                check(renamed == name) { "R8 renamed $keptClass.$name to $renamed" }
            }
        }

        var removedFrom: String? = null
        for (line in usageFile.get().asFile.readLines()) {
            if (!line.startsWith(" ")) {
                removedFrom = line.removeSuffix(":").takeIf { line.endsWith(":") && it.startsWith(ZSTD_PACKAGE) }
            } else if (removedFrom != null && "(" !in line) {
                error("R8 removed ${line.trim()} from $removedFrom")
            }
        }

        // The fields a rule must keep, which the checks above can't tell from a build that doesn't
        // use them at all.
        val seeds =
            seedsFile
                .get()
                .asFile
                .readLines()
                .toSet()
        for (field in KEPT_FIELDS) {
            check(field in seeds) { "no keep rule kept $field" }
        }
    }

    private companion object {
        const val ZSTD_PACKAGE = "com.github.luben.zstd."
        val KEPT_FIELDS =
            listOf(
                "com.github.luben.zstd.ZstdDictCompress: long nativePtr",
                "com.github.luben.zstd.ZstdDictDecompress: long nativePtr",
            )
    }
}

plugins {
    alias(libs.plugins.android.application)
}

android {
    namespace = "zip.trev.trevrpc.interop.android"
    // zstd-jni's AAR requires compiling against API 37, whose SDK platform is "android-37.0".
    compileSdk {
        version = release(37) { minorApiLevel = 0 }
    }
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

    // The scenarios run on the debug build. The two minified builds check the shrinker rules core
    // ships for its optional zstd-jni; nothing installs them.
    buildTypes {
        // An app that doesn't configure compression: it leaves out zstd-jni, so R8 fails on core's
        // references to it unless core's rules let them be.
        release {
            isMinifyEnabled = true
            proguardFiles(getDefaultProguardFile("proguard-android-optimize.txt"), "proguard-rules.pro")
        }
        // An app that configures compression: it adds zstd-jni, and verifyZstdShrinking checks that
        // core's rules keep what zstd-jni's native code looks up.
        create("zstd") {
            initWith(getByName("release"))
        }
    }

    // The scenarios read the interop dictionary from the compression golden vectors.
    sourceSets.getByName("androidTest").resources.srcDir(rootProject.layout.projectDirectory.dir("../testdata"))
}

val generateInteropClient =
    tasks.register<GenerateInteropClient>("generateInteropClient") {
        schemaRoot.set(rootProject.layout.projectDirectory.dir("../interop/proto"))
        optionsRoot.set(rootProject.layout.projectDirectory.dir("../proto"))
        schemas.set(listOf("interop.proto", "unreliable.proto", "compressed.proto", "validated.proto", "buf/validate/validate.proto"))
        generatorInstall.set(project(":protoc-gen-trevrpc-kotlin").layout.buildDirectory.dir("install/protoc-gen-trevrpc-kotlin"))
        javaOutput.set(layout.buildDirectory.dir("generated/interop/java"))
        kotlinOutput.set(layout.buildDirectory.dir("generated/interop/kotlin"))
        dependsOn(":protoc-gen-trevrpc-kotlin:installDist")
    }

androidComponents.onVariants { variant ->
    variant.sources.java?.addGeneratedSourceDirectory(generateInteropClient, GenerateInteropClient::javaOutput)
    variant.sources.kotlin?.addGeneratedSourceDirectory(generateInteropClient, GenerateInteropClient::kotlinOutput)
    if (variant.buildType == "zstd") {
        tasks.register<VerifyZstdShrinking>("verifyZstdShrinking") {
            val mapping = variant.artifacts.get(SingleArtifact.OBFUSCATION_MAPPING_FILE)
            mappingFile.set(mapping)
            usageFile.set(layout.file(mapping.map { it.asFile.resolveSibling("usage.txt") }))
            seedsFile.set(layout.file(mapping.map { it.asFile.resolveSibling("seeds.txt") }))
        }
    }
}

dependencies {
    implementation(project(":transport-cronet"))
    // The app adds the protobuf runtime its generated messages use; TrevRPC leaves it out.
    implementation(libs.protobuf.javalite)
    implementation(libs.cronet.bundled)
    // The scenarios compress with the interop dictionary; Android takes zstd-jni's AAR. The release
    // build leaves it out, as an app that doesn't configure compression does.
    debugImplementation(libs.zstd.jni) { artifact { type = "aar" } }
    "zstdImplementation"(libs.zstd.jni) { artifact { type = "aar" } }

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
