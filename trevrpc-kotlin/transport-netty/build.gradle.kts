plugins {
    kotlin("jvm")
}

kotlin {
    jvmToolchain(21)
    compilerOptions.jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_21)
}

dependencies {
    implementation(project(":core"))
    implementation(platform("io.netty:netty-bom:4.2.16.Final"))
    implementation("io.netty:netty-codec-classes-quic")
    implementation("io.netty:netty-codec-http3")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")

    testImplementation(platform("org.junit:junit-bom:5.13.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.11.0")
    testImplementation("org.bouncycastle:bcpkix-jdk18on:1.84")
    testRuntimeOnly("io.netty:netty-codec-native-quic:4.2.16.Final:linux-x86_64")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("-Dio.netty.leakDetection.level=paranoid")
}
