import org.gradle.api.attributes.Category
import org.gradle.api.attributes.LibraryElements
import org.gradle.api.attributes.Usage
import org.gradle.api.attributes.java.TargetJvmVersion

plugins {
    java
}

val trevrpcVersion =
    providers
        .gradleProperty("trevrpcVersion")
        .orElse(providers.environmentVariable("TREVRPC_VERSION"))
        .orElse(providers.gradleProperty("trevrpc.version"))
        .getOrElse("0.1.0")

val cronetPublication =
    configurations.create("cronetPublication") {
        isCanBeConsumed = false
        isCanBeResolved = true
        attributes {
            attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
            attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
            attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements.JAR))
            attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, 17)
        }
    }

dependencies {
    add(cronetPublication.name, "zip.trev.trevrpc:transport-cronet:$trevrpcVersion")
}

tasks.register("resolveCronetPublication") {
    inputs.files(cronetPublication)
    doLast {
        val files = cronetPublication.resolve()
        check(files.any { it.name == "transport-cronet-$trevrpcVersion.jar" }) {
            "Cronet transport JAR was not resolved"
        }
        check(files.none { it.name.startsWith("cronet-api-") }) {
            "Bundled Cronet API classes must not remain an external dependency"
        }
        check(files.none { it.name.startsWith("cronet-embedded-") }) {
            "Cronet provider must remain application-selected"
        }
    }
}
