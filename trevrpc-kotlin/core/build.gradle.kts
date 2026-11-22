plugins {
    kotlin("jvm")
}

java {
    toolchain.languageVersion.set(JavaLanguageVersion.of(25))
    sourceCompatibility = JavaVersion.VERSION_1_8
    targetCompatibility = JavaVersion.VERSION_1_8
}

kotlin {
    jvmToolchain(25)
    compilerOptions {
        jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_1_8)
        allWarningsAsErrors.set(true)
        freeCompilerArgs.add("-Xjdk-release=8")
    }
}

// protobuf-java, for the tests that run core's protobuf code on it as well as on protobuf-javalite.
val protobufJava =
    configurations.create("protobufJava") {
        isCanBeConsumed = false
        isCanBeResolved = true
        isTransitive = false
    }

dependencies {
    add(protobufJava.name, libs.protobuf.java)
    api(libs.coroutines.core)
    // Compiling against protobuf-javalite keeps core to the API both protobuf runtimes share. The
    // application adds the runtime its generated messages use, protobuf-javalite or protobuf-java,
    // so it stays off the published dependencies: the two runtimes hold the same classes, and a
    // dependency on either would clash with an application that uses the other.
    compileOnly(libs.protobuf.javalite)
    // Compression calls zstd-jni, which the application adds (an AAR on Android, a JAR on the JVM)
    // only if it configures compression, so it stays off the published dependencies.
    compileOnly(libs.zstd.jni)

    testImplementation(platform(libs.junit.bom))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation(libs.coroutines.test)
    testImplementation(libs.protobuf.javalite)
    testImplementation(libs.zstd.jni)
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

sourceSets.test {
    resources.srcDir("../../testdata")
}

tasks.test {
    useJUnitPlatform()
    // zstd-jni loads its native library.
    jvmArgs("--enable-native-access=ALL-UNNAMED")
    systemProperty("trevrpc.protobuf.runtime", "javalite")
}

// core compiles against protobuf-javalite, but applications run it on either runtime. This runs the
// tests of its protobuf code, compiled against protobuf-javalite as core is, on protobuf-java.
val testProtobufJava =
    tasks.register<Test>("testProtobufJava") {
        description = "Runs the protobuf tests on protobuf-java instead of protobuf-javalite."
        group = "verification"
        val test = sourceSets.test.get()
        testClassesDirs = test.output.classesDirs
        classpath = test.runtimeClasspath.filter { !it.name.startsWith("protobuf-javalite-") } + protobufJava
        useJUnitPlatform()
        filter {
            includeTestsMatching("zip.trev.trevrpc.ProtobufTest")
            includeTestsMatching("zip.trev.trevrpc.StatusTest")
        }
        systemProperty("trevrpc.protobuf.runtime", "java")
    }

tasks.check {
    dependsOn(testProtobufJava)
}
