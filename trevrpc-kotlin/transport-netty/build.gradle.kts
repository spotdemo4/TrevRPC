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

dependencies {
    implementation(project(":core"))
    implementation(platform(libs.netty.bom))
    implementation("io.netty:netty-codec-classes-quic")
    implementation("io.netty:netty-codec-http3")
    implementation(libs.coroutines.core)

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(libs.coroutines.test)
    testImplementation(libs.bouncycastle.bcpkix)
    testRuntimeOnly("io.netty:netty-codec-native-quic:${libs.versions.netty.get()}:linux-x86_64")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.test {
    useJUnitPlatform()
    jvmArgs("-Dio.netty.leakDetection.level=paranoid", "--enable-native-access=ALL-UNNAMED")
}
