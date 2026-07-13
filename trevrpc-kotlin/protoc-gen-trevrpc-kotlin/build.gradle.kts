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

dependencies {
    implementation(libs.protobuf.java)

    testImplementation(project(":core"))
    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(libs.kotlin.compiler.embeddable)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
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
