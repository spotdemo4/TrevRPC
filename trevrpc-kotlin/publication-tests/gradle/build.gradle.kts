import org.gradle.api.attributes.Category
import org.gradle.api.attributes.LibraryElements
import org.gradle.api.attributes.Usage
import org.gradle.api.attributes.java.TargetJvmVersion

plugins {
    kotlin("jvm") version "2.4.10" apply false
    id("com.google.protobuf") version "0.10.0" apply false
}

val metadataSource = providers.gradleProperty("trevrpc.metadata").orElse("gradle")

val trevrpcVersion =
    providers
        .gradleProperty("trevrpcVersion")
        .orElse(providers.environmentVariable("TREVRPC_VERSION"))
        .orElse(providers.gradleProperty("trevrpc.version"))
        .getOrElse("0.1.0")

val verifyRejectedJvmVariants =
    tasks.register("verifyRejectedJvmVariants") {
        onlyIf { metadataSource.get() == "gradle" }
        doLast {
            fun expectRejected(
                name: String,
                target: Int,
                dependency: String,
            ) {
                val configuration =
                    configurations.create(name) {
                        isCanBeConsumed = false
                        isCanBeResolved = true
                        attributes {
                            attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
                            attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
                            attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.JAR))
                            attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, target)
                        }
                    }
                dependencies.add(name, dependency)
                val failure = runCatching { configuration.resolve() }.exceptionOrNull()
                check(failure != null) { "$dependency unexpectedly resolved for target JVM $target" }
                val messages =
                    generateSequence(failure as Throwable?) { it.cause }
                        .mapNotNull(Throwable::message)
                        .joinToString("\n")
                check(
                    messages.contains("compatible with JVM runtime version $target") &&
                        messages.contains("only compatible with JVM runtime version 21"),
                ) {
                    "$dependency failed for an unexpected reason:\n$messages"
                }
            }

            expectRejected("nettyOnJava8", 8, "zip.trev.trevrpc:transport-netty:$trevrpcVersion")
            expectRejected("nettyOnJava17", 17, "zip.trev.trevrpc:transport-netty:$trevrpcVersion")
            expectRejected("generatorOnJava8", 8, "zip.trev.trevrpc:protoc-gen-trevrpc-kotlin:$trevrpcVersion")
            expectRejected("generatorOnJava17", 17, "zip.trev.trevrpc:protoc-gen-trevrpc-kotlin:$trevrpcVersion")
        }
    }

tasks.register("verifyConsumers") {
    dependsOn(
        ":generated-consumer:compileKotlin",
        ":netty-consumer:compileKotlin",
        ":resolution-consumer:resolveCronetPublication",
        verifyRejectedJvmVariants,
    )
}
