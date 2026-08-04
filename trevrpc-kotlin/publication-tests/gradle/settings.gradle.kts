pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
    resolutionStrategy {
        eachPlugin {
            if (requested.id.id == "com.google.protobuf") {
                useModule("com.google.protobuf:protobuf-gradle-plugin:${requested.version}")
            }
        }
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        val stagedRepository = providers.gradleProperty("trevrpc.repository").get()
        val metadataSource = providers.gradleProperty("trevrpc.metadata").orElse("gradle").get()
        exclusiveContent {
            forRepository {
                maven {
                    name = "trevrpcStaging"
                    url = uri(stagedRepository)
                    metadataSources {
                        when (metadataSource) {
                            "gradle" -> gradleMetadata()
                            "pom" -> mavenPom()
                            else -> error("unsupported trevrpc.metadata=$metadataSource")
                        }
                    }
                }
            }
            filter { includeGroup("zip.trev.trevrpc") }
        }
        exclusiveContent {
            forRepository { google() }
            filter { includeGroup("org.chromium.net") }
        }
        mavenCentral()
    }
}

rootProject.name = "trevrpc-kotlin-publication-consumer"
include("generated-consumer", "netty-consumer", "resolution-consumer")
