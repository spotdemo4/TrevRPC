import org.gradle.api.DefaultTask
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import java.util.zip.ZipFile

/**
 * Extracts `classes.jar` from the Cronet AAR so this plain JVM module can compile and test against
 * the Cronet API. Cronet ships its API and implementation in one Android library, so the classes stay
 * off the published JAR and the application supplies Cronet with its provider.
 */
abstract class ExtractCronetClasses : DefaultTask() {
    @get:InputFile
    @get:PathSensitive(PathSensitivity.NONE)
    abstract val inputAar: RegularFileProperty

    @get:OutputFile
    abstract val outputJar: RegularFileProperty

    @TaskAction
    fun extract() {
        val output = outputJar.get().asFile
        output.parentFile.mkdirs()
        ZipFile(inputAar.get().asFile).use { archive ->
            val entry = checkNotNull(archive.getEntry("classes.jar"))
            archive.getInputStream(entry).use { input ->
                output.outputStream().use(input::copyTo)
            }
        }
    }
}

plugins {
    kotlin("jvm")
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(25))
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
}

kotlin {
    jvmToolchain(25)
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17)
        allWarningsAsErrors.set(true)
        freeCompilerArgs.add("-Xjdk-release=17")
    }
}

val cronetAar =
    configurations.create("cronetAar") {
        isCanBeConsumed = false
        isCanBeResolved = true
        isTransitive = false
    }
val cronetClassesJar = layout.buildDirectory.file("cronet/classes.jar")
val extractCronetClasses =
    tasks.register<ExtractCronetClasses>("extractCronetClasses") {
        inputAar.set(layout.file(providers.provider { cronetAar.singleFile }))
        outputJar.set(cronetClassesJar)
    }

dependencies {
    add(cronetAar.name, libs.cronet.library)
    api(project(":core"))
    implementation(libs.coroutines.core)
    compileOnly(files(cronetClassesJar).builtBy(extractCronetClasses))

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(files(cronetClassesJar).builtBy(extractCronetClasses))
    testImplementation(libs.coroutines.test)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}
