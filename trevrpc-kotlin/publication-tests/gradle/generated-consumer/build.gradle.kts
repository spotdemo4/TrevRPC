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

val trevrpcVersion =
    providers
        .gradleProperty("trevrpcVersion")
        .orElse(providers.environmentVariable("TREVRPC_VERSION"))
        .orElse(providers.gradleProperty("trevrpc.version"))
        .orElse(
            providers
                .fileContents(layout.projectDirectory.file("../../../build.gradle.kts"))
                .asText
                .map { text ->
                    Regex("""version = "([0-9]+\.[0-9]+\.[0-9]+)"""")
                        .find(text)
                        ?.groupValues
                        ?.get(1) ?: ""
                }.filter { it.isNotEmpty() },
        ).getOrElse("0.1.2")

dependencies {
    implementation("zip.trev.trevrpc:core:$trevrpcVersion")
}

val generator =
    configurations.create("trevrpcGenerator") {
        isCanBeConsumed = false
        isCanBeResolved = true
        isTransitive = false
    }
dependencies.add(generator.name, "zip.trev.trevrpc:protoc-gen-trevrpc-kotlin:$trevrpcVersion:jdk21@jar")

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
            artifact = "com.google.protobuf:protoc:4.36.0"
        }
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
