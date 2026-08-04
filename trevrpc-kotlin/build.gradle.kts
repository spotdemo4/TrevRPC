import org.gradle.api.publish.PublishingExtension
import org.gradle.api.publish.maven.MavenPublication
import org.gradle.api.publish.maven.tasks.PublishToMavenLocal
import org.gradle.api.tasks.Delete
import org.gradle.api.tasks.Exec
import org.gradle.jvm.tasks.Jar

plugins {
    alias(libs.plugins.dokka) apply false
    alias(libs.plugins.kotlin.jvm) apply false
}

allprojects {
    group = "zip.trev.trevrpc"
    version = "0.1.0"

    configurations.configureEach {
        resolutionStrategy.eachDependency {
            val requestedVersion = requested.version.orEmpty()
            if (
                requested.group == "org.jetbrains.kotlin" &&
                listOf("alpha", "beta", "rc", "snapshot").any { requestedVersion.contains(it, ignoreCase = true) }
            ) {
                useVersion(libs.versions.kotlin.get())
                because("TrevRPC uses stable Kotlin releases only")
            }
        }
    }

    dependencyLocking {
        lockAllConfigurations()
    }

    tasks.withType<PublishToMavenLocal>().configureEach {
        enabled = false
    }
}

subprojects {
    // Tests run on JDK 25 even where published artifacts retain Java 8 compatibility.
    afterEvaluate {
        listOf("testCompileClasspath", "testRuntimeClasspath").forEach { configurationName ->
            configurations.named(configurationName) {
                attributes.attribute(
                    org.gradle.api.attributes.java.TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE,
                    25,
                )
            }
        }
    }

    tasks.register("resolveAndLockAll") {
        notCompatibleWithConfigurationCache("Resolves configurations at execution time")
        doFirst {
            require(gradle.startParameter.isWriteDependencyLocks) {
                "$path must be run with --write-locks"
            }
        }
        doLast {
            configurations.filter { it.isCanBeResolved }.forEach { it.resolve() }
        }
    }
}

val stagingRepository = layout.buildDirectory.dir("staging-repository")
val publishableModules =
    linkedMapOf(
        "core" to ("TrevRPC Kotlin Core" to "Coroutine-first RPC runtime and protobuf framing for TrevRPC."),
        "transport-netty" to
            (
                "TrevRPC Kotlin Netty Transport" to
                    "JDK 21 QUIC, HTTP/3, and WebTransport client and server transport backed by Netty."
            ),
        "transport-cronet" to
            (
                "TrevRPC Kotlin Cronet Transport" to
                    "JVM 17 Cronet HTTP/3 API adapter with application-owned provider resources."
            ),
        "protoc-gen-trevrpc-kotlin" to
            (
                "TrevRPC Kotlin Protobuf Generator" to
                    "JDK 21 protobuf service generator for the TrevRPC Kotlin runtime."
            ),
    )

publishableModules.forEach { (moduleName, metadata) ->
    project(":$moduleName") {
        pluginManager.apply("java-library")
        pluginManager.apply("maven-publish")
        pluginManager.apply("org.jetbrains.dokka")

        tasks.matching { it.name.startsWith("dokka") }.configureEach {
            notCompatibleWithConfigurationCache("Dokka resolves source-set classpaths during documentation generation")
        }

        tasks.withType<Jar>().configureEach {
            isPreserveFileTimestamps = false
            isReproducibleFileOrder = true
            from(rootProject.layout.projectDirectory.file("../LICENSE")) {
                into("META-INF")
                rename { "LICENSE" }
            }
        }

        val dokkaJavadocJar =
            tasks.register<Jar>("dokkaJavadocJar") {
                dependsOn(tasks.named("dokkaGeneratePublicationHtml"))
                archiveClassifier.set("javadoc")
                from(tasks.named("dokkaGeneratePublicationHtml"))
            }

        extensions.configure<PublishingExtension> {
            publications {
                create<MavenPublication>("maven") {
                    artifactId = moduleName
                    from(components["java"])
                    artifact(dokkaJavadocJar)
                    pom {
                        name.set(metadata.first)
                        description.set(metadata.second)
                        url.set("https://trev.zip/llc/TrevRPC")
                        inceptionYear.set("2026")
                        licenses {
                            license {
                                name.set("MIT License")
                                url.set("https://opensource.org/license/mit")
                                distribution.set("repo")
                            }
                        }
                        developers {
                            developer {
                                id.set("trev")
                                name.set("Trev")
                                email.set("me@trev.xyz")
                                url.set("https://trev.xyz")
                            }
                        }
                        scm {
                            connection.set("scm:git:https://trev.zip/llc/TrevRPC.git")
                            developerConnection.set("scm:git:https://trev.zip/llc/TrevRPC.git")
                            url.set("https://trev.zip/llc/TrevRPC")
                        }
                    }
                }
            }
            repositories {
                maven {
                    name = "staging"
                    url = uri(stagingRepository)
                }
            }
        }

        pluginManager.withPlugin("org.jetbrains.kotlin.jvm") {
            extensions.configure<PublishingExtension> {
                publications.named("maven", MavenPublication::class) {
                    artifact(tasks.named("kotlinSourcesJar"))
                }
            }
        }
    }
}

val cleanStagingRepository =
    tasks.register<Delete>("cleanStagingRepository") {
        delete(stagingRepository)
    }

val stagedPublicationTasks =
    publishableModules.keys.map { moduleName ->
        ":$moduleName:publishMavenPublicationToStagingRepository"
    }

tasks.register("stageMavenRepository") {
    group = "publishing"
    description = "Stages the four TrevRPC Kotlin artifacts in a repository-local Maven repository."
    dependsOn(cleanStagingRepository)
    dependsOn(stagedPublicationTasks)
}

val verifyStagedMavenRepository =
    tasks.register<Exec>("verifyStagedMavenRepository") {
        group = "verification"
        description = "Validates the staged Maven repository, metadata, archives, and executable generator."
        dependsOn("stageMavenRepository")
        commandLine(
            "python3",
            layout.projectDirectory.file("publication-tests/verify_staged_repository.py").asFile,
            stagingRepository.get().asFile,
        )
    }

tasks.register<Exec>("verifyGradleConsumers") {
    group = "verification"
    description = "Compiles isolated Gradle consumers against Gradle metadata and Maven POM metadata."
    dependsOn(verifyStagedMavenRepository)
    environment("TREVRPC_STAGING_REPOSITORY", stagingRepository.get().asFile.absolutePath)
    commandLine("bash", layout.projectDirectory.file("publication-tests/gradle/verify.sh").asFile)
}

tasks.register<Exec>("verifyMavenConsumers") {
    group = "verification"
    description = "Compiles isolated Maven consumers using an isolated local repository."
    dependsOn(verifyStagedMavenRepository)
    environment("TREVRPC_STAGING_REPOSITORY", stagingRepository.get().asFile.absolutePath)
    commandLine("bash", layout.projectDirectory.file("publication-tests/maven/verify.sh").asFile)
}

tasks.register<Exec>("verifyCronetConsumers") {
    group = "verification"
    description = "Validates and compiles isolated JVM 17 Cronet consumers with Gradle and Maven."
    dependsOn(verifyStagedMavenRepository)
    environment("TREVRPC_STAGING_REPOSITORY", stagingRepository.get().asFile.absolutePath)
    commandLine("bash", layout.projectDirectory.file("publication-tests/cronet/verify.sh").asFile)
}

gradle.projectsEvaluated {
    stagedPublicationTasks.forEach { taskPath ->
        tasks.getByPath(taskPath).mustRunAfter(cleanStagingRepository)
    }
}
