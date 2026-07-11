plugins {
    application
    kotlin("jvm")
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(21))
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

kotlin {
    jvmToolchain(21)
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
    implementation("com.google.protobuf:protobuf-java:4.35.1")

    testImplementation(project(":core"))
    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.jetbrains.kotlin:kotlin-compiler-embeddable:2.4.0")
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
