plugins {
    kotlin("jvm") version "2.4.0" apply false
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
                useVersion("2.4.0")
                because("TrevRPC uses stable Kotlin releases only")
            }
        }
    }

    dependencyLocking {
        lockAllConfigurations()
    }
}
