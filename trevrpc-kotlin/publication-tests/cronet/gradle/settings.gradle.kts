pluginManagement {
    repositories {
        gradlePluginPortal()
        val mirror = System.getenv("TREVRPC_GRADLE_MAVEN_CENTRAL")
        if (mirror == null) {
            mavenCentral()
        } else {
            maven { url = uri(mirror) }
        }
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        maven {
            url = uri(providers.gradleProperty("trevrpc.repository").get())
        }
        val mirror = System.getenv("TREVRPC_GRADLE_MAVEN_CENTRAL")
        if (mirror == null) {
            mavenCentral()
        } else {
            maven { url = uri(mirror) }
        }
    }
}

rootProject.name = "trevrpc-cronet-publication-consumer"
