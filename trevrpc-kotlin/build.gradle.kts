plugins {
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
