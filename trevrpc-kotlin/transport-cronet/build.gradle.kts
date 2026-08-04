import org.gradle.api.DefaultTask
import org.gradle.api.file.RegularFileProperty
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
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
    api(project(":core"))
    implementation(libs.coroutines.core)
    compileOnly(files(cronetApiJar).builtBy(prepareCronetApi))

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(files(cronetApiJar).builtBy(prepareCronetApi))
    testImplementation(libs.coroutines.test)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.jar {
    dependsOn(prepareCronetApi)
    from({ zipTree(cronetApiJar.get().asFile) }) {
        include("org/chromium/net/**")
    }
}

tasks.test {
    useJUnitPlatform()
}

pluginManager.withPlugin("maven-publish") {
    extensions.configure<PublishingExtension> {
        publications.named("maven", MavenPublication::class) {
            pom {
                description.set("JVM 17 Cronet HTTP/3 API adapter with application-owned provider resources.")
                licenses {
                    license {
                        name.set("Chromium and built-in dependencies")
                        url.set("https://storage.googleapis.com/chromium-cronet/android/143.0.7445.0/Release/cronet/LICENSE")
                        distribution.set("repo")
                    }
                }
            }
        }
    }
}
