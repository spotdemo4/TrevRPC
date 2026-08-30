import org.gradle.api.file.DuplicatesStrategy
import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.jvm.tasks.Jar

plugins {
    application
    kotlin("jvm")
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
}

application {
    mainClass.set("zip.trev.trevrpc.generator.MainKt")
    applicationName = "protoc-gen-trevrpc-kotlin"
}

tasks.jar {
    manifest.attributes["Main-Class"] = application.mainClass.get()
}

dependencies {
    implementation(libs.protobuf.java)

    testImplementation(project(":core"))
    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(libs.kotlin.compiler.embeddable)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

val protobufVersion =
    libs.versions.protobuf
        .asProvider()
        .get()
val kotlinVersion = libs.versions.kotlin.get()

val executableJar =
    tasks.register<Jar>("executableJar") {
        archiveClassifier.set("jdk21")
        duplicatesStrategy = DuplicatesStrategy.EXCLUDE
        manifest.attributes["Main-Class"] = application.mainClass.get()
        exclude("META-INF/*.DSA", "META-INF/*.RSA", "META-INF/*.SF")
        inputs.property("protobufVersion", protobufVersion)
        inputs.property("kotlinVersion", kotlinVersion)
        from(sourceSets.main.get().output)
        from(layout.projectDirectory.dir("src/executable/resources")) {
            exclude("META-INF/third-party/THIRD-PARTY.txt")
        }
        from(
            layout.projectDirectory.file(
                "src/executable/resources/META-INF/third-party/THIRD-PARTY.txt",
            ),
        ) {
            into("META-INF/third-party")
            expand(
                mapOf(
                    "protobufVersion" to protobufVersion,
                    "kotlinVersion" to kotlinVersion,
                ),
            )
        }
        from(
            configurations.runtimeClasspath.map { classpath ->
                classpath.map { entry -> if (entry.isDirectory) entry else zipTree(entry) }
            },
        )
    }

extensions.configure<PublishingExtension> {
    publications.named("maven", MavenPublication::class) {
        artifact(executableJar)
    }
}

tasks.test {
    dependsOn(tasks.installDist)
    useJUnitPlatform()
    systemProperty(
        "trevrpc.kotlin.plugin",
        layout.buildDirectory
            .file("install/protoc-gen-trevrpc-kotlin/bin/protoc-gen-trevrpc-kotlin")
            .get()
            .asFile,
    )
}
