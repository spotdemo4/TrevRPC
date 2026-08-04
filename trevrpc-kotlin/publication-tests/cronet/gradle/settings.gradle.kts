pluginManagement {
    repositories {
        gradlePluginPortal()
        mavenCentral()
    }
}

dependencyResolutionManagement {
    repositoriesMode.set(RepositoriesMode.FAIL_ON_PROJECT_REPOS)
    repositories {
        maven {
            url = uri(providers.gradleProperty("trevrpc.repository").get())
        }
        mavenCentral()
    }
}

rootProject.name = "trevrpc-cronet-publication-consumer"
