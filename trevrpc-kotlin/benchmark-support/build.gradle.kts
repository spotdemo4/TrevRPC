import com.google.protobuf.gradle.id

plugins {
    kotlin("jvm")
    alias(libs.plugins.protobuf)
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
        freeCompilerArgs.add("-Xjdk-release=17")
        allWarningsAsErrors.set(true)
    }
    sourceSets.main {
        kotlin.srcDir(layout.buildDirectory.dir("generated/sources/proto/main/trevrpc-kotlin"))
    }
}

val canonicalBenchmarkProto = rootProject.layout.projectDirectory.file("../bench/proto/benchmark.proto")
val packagedBenchmarkProto = layout.projectDirectory.file("src/main/proto/benchmark.proto")
val benchmarkProtoSource =
    if (canonicalBenchmarkProto.asFile.isFile) {
        canonicalBenchmarkProto.asFile.parentFile
    } else {
        packagedBenchmarkProto.asFile.parentFile
    }

sourceSets {
    main {
        proto.setSrcDirs(listOf(benchmarkProtoSource))
    }
}

dependencies {
    api(project(":core"))
    api(libs.protobuf.java)
    api(libs.coroutines.core)

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

val trevrpcPlugin =
    project(":protoc-gen-trevrpc-kotlin")
        .layout.buildDirectory
        .file("install/protoc-gen-trevrpc-kotlin/bin/protoc-gen-trevrpc-kotlin")
val configuredProtoc = providers.gradleProperty("trevrpcProtocPath")

protobuf {
    protoc {
        if (configuredProtoc.isPresent) {
            val protocFile = rootProject.file(configuredProtoc.get())
            require(protocFile.isFile && protocFile.canExecute()) {
                "trevrpcProtocPath must name an executable protoc binary: $protocFile"
            }
            path = protocFile.absolutePath
        } else {
            artifact =
                libs.protobuf.protoc
                    .get()
                    .toString()
        }
    }
    plugins {
        id("trevrpc-kotlin") {
            path = trevrpcPlugin.get().asFile.absolutePath
        }
    }
    generateProtoTasks {
        all().configureEach {
            dependsOn(":protoc-gen-trevrpc-kotlin:installDist")
            plugins {
                id("trevrpc-kotlin")
            }
        }
    }
}

tasks.test {
    useJUnitPlatform()
}
