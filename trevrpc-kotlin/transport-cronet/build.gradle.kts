import org.gradle.api.DefaultTask
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.provider.Property
import org.gradle.api.tasks.Input
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.TaskAction
import java.net.URI
import java.security.MessageDigest
import java.util.zip.ZipFile

abstract class PrepareCronetApi : DefaultTask() {
    @get:Input
    abstract val version: Property<String>

    @get:Input
    abstract val sourceUrl: Property<String>

    @get:Input
    abstract val sha256: Property<String>

    @get:OutputFile
    abstract val outputJar: RegularFileProperty

    @TaskAction
    fun prepare() {
        val artifactVersion = version.get()
        val aar = temporaryDir.resolve("cronet-api-$artifactVersion.aar")
        URI(sourceUrl.get()).toURL().openStream().use { input ->
            aar.outputStream().use(input::copyTo)
        }
        val digest =
            MessageDigest
                .getInstance("SHA-256")
                .digest(aar.readBytes())
                .joinToString("") { (it.toInt() and 0xff).toString(16).padStart(2, '0') }
        check(digest == sha256.get()) { "cronet-api checksum mismatch: $digest" }
        val output = outputJar.get().asFile
        output.parentFile.mkdirs()
        ZipFile(aar).use { archive ->
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

// cronet-api is an API-only AAR hosted on Google Maven. The root intentionally exposes only
// Maven Central, so extract its classes.jar here without adding a repository or a runtime provider.
val cronetApiVersion = "143.7445.0"
val cronetApiAarUrl =
    "https://dl.google.com/dl/android/maven2/org/chromium/net/cronet-api/$cronetApiVersion/cronet-api-$cronetApiVersion.aar"
val cronetApiSha256 = "8397f5b3752e3387571a0ff33e19a0db4cc14da16f4337872dcf3aa66ad9372a"
val cronetApiJar = layout.buildDirectory.file("cronet-api/$cronetApiVersion/classes.jar")
val prepareCronetApi by
    tasks.registering(PrepareCronetApi::class) {
        version.set(cronetApiVersion)
        sourceUrl.set(cronetApiAarUrl)
        sha256.set(cronetApiSha256)
        outputJar.set(cronetApiJar)
    }

dependencies {
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
