import org.gradle.api.DefaultTask
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.tasks.InputFile
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.PathSensitive
import org.gradle.api.tasks.PathSensitivity
import org.gradle.api.tasks.TaskAction
import java.util.zip.ZipFile

abstract class PrepareCronetApi : DefaultTask() {
    @get:InputFile
    @get:PathSensitive(PathSensitivity.NONE)
    abstract val inputAar: RegularFileProperty

    @get:OutputFile
    abstract val outputJar: RegularFileProperty

    @TaskAction
    fun prepare() {
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
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

kotlin {
    jvmToolchain(25)
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_1_8)
        allWarningsAsErrors.set(true)
        freeCompilerArgs.add("-Xjdk-release=8")
    }
}

val cronetApiAar =
    configurations.create("cronetApiAar") {
        isCanBeConsumed = false
        isCanBeResolved = true
        isTransitive = false
    }
val cronetApiJar = layout.buildDirectory.file("cronet-api/classes.jar")
val prepareCronetApi =
    tasks.register<PrepareCronetApi>("prepareCronetApi") {
        inputAar.set(layout.file(providers.provider { cronetApiAar.singleFile }))
        outputJar.set(cronetApiJar)
    }

dependencies {
    add(cronetApiAar.name, libs.cronet.api)
    implementation(project(":core"))
    implementation(libs.coroutines.core)
    compileOnly(files(cronetApiJar).builtBy(prepareCronetApi))

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(libs.coroutines.test)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
}
