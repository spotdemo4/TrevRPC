import com.google.protobuf.gradle.id

plugins {
    kotlin("jvm")
    id("com.google.protobuf")
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
        freeCompilerArgs.add("-Xjdk-release=8")
    }
    sourceSets.main {
        kotlin.srcDir(layout.buildDirectory.dir("generated/sources/proto/main/trevrpc-kotlin"))
        kotlin.srcDir("../../sources/generated")
    }
}

sourceSets.main {
    proto.srcDir("../../proto")
}

dependencies {
    implementation("zip.trev.trevrpc:core:0.1.0")
}

val generator =
    configurations.create("trevrpcGenerator") {
        isCanBeConsumed = false
        isCanBeResolved = true
        isTransitive = false
    }
dependencies.add(generator.name, "zip.trev.trevrpc:protoc-gen-trevrpc-kotlin:0.1.0:jdk21@jar")

val generatorWrapper = layout.buildDirectory.file("tools/protoc-gen-trevrpc-kotlin")
val prepareGenerator =
    tasks.register("prepareGenerator") {
        inputs.files(generator)
        outputs.file(generatorWrapper)
        doLast {
            val wrapper = generatorWrapper.get().asFile
            wrapper.parentFile.mkdirs()
            wrapper.writeText(
                "#!/bin/sh\nexec \"${System.getProperty("java.home")}/bin/java\" -jar \"${generator.singleFile}\" \"\$@\"\n",
            )
            check(wrapper.setExecutable(true)) { "failed to make generator wrapper executable" }
        }
    }

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:4.35.1"
    }
    plugins {
        id("trevrpc-kotlin") {
            path = generatorWrapper.get().asFile.absolutePath
        }
    }
    generateProtoTasks {
        all().configureEach {
            dependsOn(prepareGenerator)
            plugins {
                id("trevrpc-kotlin")
            }
        }
    }
}

tasks.compileKotlin {
    dependsOn(tasks.named("generateProto"))
}
