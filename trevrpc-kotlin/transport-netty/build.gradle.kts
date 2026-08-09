plugins {
    kotlin("jvm")
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(25))
    sourceCompatibility = JavaVersion.VERSION_21
    targetCompatibility = JavaVersion.VERSION_21
}

kotlin {
    jvmToolchain(25)
    compilerOptions.jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
}

val supportedNettyNativeClassifiers =
    listOf(
        "linux-x86_64",
        "linux-aarch_64",
        "osx-x86_64",
        "osx-aarch_64",
        "windows-x86_64",
    )
val configuredNettyNativeClassifier = providers.gradleProperty("trevrpcNettyNativeClassifier")
val nettyNativeClassifiers =
    configuredNettyNativeClassifier
        .map { classifier ->
            require(classifier in supportedNettyNativeClassifiers) {
                "Unsupported trevrpcNettyNativeClassifier '$classifier'; expected one of " +
                    supportedNettyNativeClassifiers.joinToString()
            }
            listOf(classifier)
        }.orElse(supportedNettyNativeClassifiers)

dependencies {
    api(project(":core"))
    api(platform(libs.netty.bom))
    api("io.netty:netty-codec-classes-quic")
    api("io.netty:netty-codec-http3") {
        exclude(group = "io.netty", module = "netty-codec-native-quic")
    }
    implementation(libs.coroutines.core)

    nettyNativeClassifiers.get().forEach { classifier ->
        runtimeOnly("io.netty:netty-codec-native-quic:${libs.versions.netty.get()}:$classifier")
    }

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(libs.coroutines.test)
    testImplementation(libs.bouncycastle.bcpkix)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("-Dio.netty.leakDetection.level=paranoid", "--enable-native-access=ALL-UNNAMED")
}
