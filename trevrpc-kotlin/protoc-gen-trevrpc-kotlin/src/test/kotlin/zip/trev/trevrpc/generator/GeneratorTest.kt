package zip.trev.trevrpc.generator

import com.google.protobuf.DescriptorProtos.DescriptorProto
import com.google.protobuf.DescriptorProtos.FileDescriptorProto
import com.google.protobuf.DescriptorProtos.FileOptions
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse
import org.jetbrains.kotlin.cli.common.ExitCode
import org.jetbrains.kotlin.cli.common.messages.MessageRenderer
import org.jetbrains.kotlin.cli.common.messages.PrintingMessageCollector
import org.jetbrains.kotlin.cli.jvm.K2JVMCompiler
import org.jetbrains.kotlin.config.Services
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.PrintStream
import java.nio.file.Files
import java.nio.file.Path
import javax.tools.ToolProvider
import kotlin.io.path.createDirectories
import kotlin.io.path.extension
import kotlin.io.path.isRegularFile
import kotlin.io.path.readText

class GeneratorTest {
    @Test
    fun `generates golden API for all RPC shapes`() {
        val response = generate(shapesRequest())

        assertEquals("", response.error)
        assertEquals("api/greeter.trevrpc.kt", response.fileList.single().name)
        assertEquals(golden("all-shapes.txt"), snapshot(response.fileList.single().content))
    }

    @Test
    fun `generates golden API for options imports nested messages collisions and escaping`() {
        val response = generate(collisionsRequest())

        assertEquals("", response.error)
        assertEquals("api/collision.rpc.kt", response.fileList.single().name)
        assertEquals(golden("collisions.txt"), snapshot(response.fileList.single().content))
    }

    @Test
    fun `rejects unknown options in the protocol response`() {
        val response = generate(CodeGeneratorRequest.newBuilder().setParameter("unknown=value").build())

        assertEquals("unknown trevrpc-kotlin option \"unknown\"", response.error)
        assertTrue(response.fileList.isEmpty())
    }

    @Test
    fun `round trips the protoc plugin protocol`() {
        val input = shapesRequest().toByteArray()
        val output = ByteArrayOutputStream()

        runPlugin(ByteArrayInputStream(input), output)

        val response = CodeGeneratorResponse.parseFrom(output.toByteArray())
        assertEquals("", response.error)
        assertEquals(1, response.fileCount)
    }

    private fun golden(name: String): String =
        checkNotNull(javaClass.getResource("/golden/$name")) { "missing golden resource $name" }
            .readText()
            .trimEnd()

    private fun snapshot(content: String): String =
        content
            .lineSequence()
            .filter { line -> SNAPSHOT_MARKERS.any(line::contains) }
            .joinToString("\n")
            .trimEnd()

    private fun shapesRequest(): CodeGeneratorRequest {
        val file =
            FileDescriptorProto
                .newBuilder()
                .setName("api/greeter.proto")
                .setPackage("hello.v1")
                .setOptions(
                    FileOptions
                        .newBuilder()
                        .setJavaPackage("example.generated")
                        .setJavaMultipleFiles(true),
                ).addMessageType(message("HelloRequest"))
                .addMessageType(message("HelloReply"))
                .addService(
                    ServiceDescriptorProto
                        .newBuilder()
                        .setName("Greeter")
                        .addMethod(method("SayHello", ".hello.v1.HelloRequest", ".hello.v1.HelloReply"))
                        .addMethod(
                            method("LotsOfReplies", ".hello.v1.HelloRequest", ".hello.v1.HelloReply")
                                .setServerStreaming(true),
                        ).addMethod(
                            method("LotsOfGreetings", ".hello.v1.HelloRequest", ".hello.v1.HelloReply")
                                .setClientStreaming(true),
                        ).addMethod(
                            method("BidiHello", ".hello.v1.HelloRequest", ".hello.v1.HelloReply")
                                .setClientStreaming(true)
                                .setServerStreaming(true),
                        ),
                ).build()
        return CodeGeneratorRequest
            .newBuilder()
            .addFileToGenerate(file.name)
            .addProtoFile(file)
            .build()
    }

    private fun collisionsRequest(): CodeGeneratorRequest {
        val imported =
            FileDescriptorProto
                .newBuilder()
                .setName("shared/common.proto")
                .setPackage("shared")
                .setOptions(
                    FileOptions
                        .newBuilder()
                        .setJavaPackage("shared.when")
                        .setJavaMultipleFiles(true),
                ).addMessageType(message("Result"))
                .build()
        val envelope =
            message("Envelope")
                .toBuilder()
                .addNestedType(message("Result"))
                .build()
        val target =
            FileDescriptorProto
                .newBuilder()
                .setName("api/collision.proto")
                .setPackage("api")
                .setOptions(
                    FileOptions
                        .newBuilder()
                        .setJavaPackage("example.class")
                        .setJavaOuterClassname("ApiTypes"),
                ).addDependency(imported.name)
                .addMessageType(envelope)
                .addMessageType(message("Result"))
                .addService(
                    ServiceDescriptorProto
                        .newBuilder()
                        .setName("When")
                        .addMethod(method("When", ".shared.Result", ".api.Envelope.Result"))
                        .addMethod(method("Nested", ".api.Result", ".shared.Result")),
                ).build()
        return CodeGeneratorRequest
            .newBuilder()
            .setParameter("runtime_package=custom.rpc,file_suffix=.rpc.kt")
            .addFileToGenerate(target.name)
            .addProtoFile(imported)
            .addProtoFile(target)
            .build()
    }

    private fun message(name: String): DescriptorProto = DescriptorProto.newBuilder().setName(name).build()

    private fun method(
        name: String,
        input: String,
        output: String,
    ): MethodDescriptorProto.Builder =
        MethodDescriptorProto
            .newBuilder()
            .setName(name)
            .setInputType(input)
            .setOutputType(output)

    private companion object {
        val SNAPSHOT_MARKERS =
            listOf(
                "package ",
                "MessageCodec<",
                "parseFrom(",
                "invalidArgument(",
                "const val ",
                "_RPC_KIND:",
                "interface ",
                "suspend fun ",
                " fun ",
                "class ",
                "Call(",
                "Flow<",
                "client.",
                "server.route",
                "requireTrevrpcKind(",
                "decodeTrevrpcRequest(",
                ".map(",
                "ResponseEnvelope(",
            )
    }
}

class GeneratedCodeCompilationTest {
    @Test
    fun `protoc generated four shape Greeter code compiles against core and protobuf`(
        @TempDir temp: Path,
    ) {
        val schemaPath = "trevrpc-rust/crates/protoc-gen-trevrpc-rust/tests/proto/greeter.proto"
        val schema =
            generateSequence(Path.of(System.getProperty("user.dir")).toAbsolutePath(), Path::getParent)
                .map { it.resolve(schemaPath) }
                .firstOrNull(Path::isRegularFile)
                ?: error("Greeter schema not found from ${System.getProperty("user.dir")}")
        check(schema.isRegularFile()) { "Greeter schema not found at $schema" }
        val javaSources = temp.resolve("java").createDirectories()
        val kotlinSources = temp.resolve("kotlin").createDirectories()
        val plugin = Path.of(checkNotNull(System.getProperty("trevrpc.kotlin.plugin")))

        runCommand(
            listOf(
                "protoc",
                "--proto_path=${schema.parent}",
                "--java_out=$javaSources",
                "--plugin=protoc-gen-trevrpc-kotlin=$plugin",
                "--trevrpc-kotlin_out=$kotlinSources",
                schema.fileName.toString(),
            ),
            schema.parent,
        )

        val classpath = System.getProperty("java.class.path")
        val javaClasses = temp.resolve("java-classes").createDirectories()
        compileJava(sourceFiles(javaSources, "java"), javaClasses, classpath)
        val kotlinClasses = temp.resolve("kotlin-classes").createDirectories()
        compileKotlin(sourceFiles(kotlinSources, "kt"), kotlinClasses, "$javaClasses${java.io.File.pathSeparator}$classpath")
    }

    private fun runCommand(
        command: List<String>,
        directory: Path,
    ) {
        val process =
            ProcessBuilder(command)
                .directory(directory.toFile())
                .redirectErrorStream(true)
                .start()
        val output = process.inputStream.bufferedReader().readText()
        val status = process.waitFor()
        check(status == 0) { "command failed ($status): ${command.joinToString(" ")}\n$output" }
    }

    private fun sourceFiles(
        root: Path,
        extension: String,
    ): List<Path> =
        Files.walk(root).use { paths ->
            paths.filter { it.isRegularFile() && it.extension == extension }.toList()
        }

    private fun compileJava(
        sources: List<Path>,
        destination: Path,
        classpath: String,
    ) {
        val errors = ByteArrayOutputStream()
        val arguments =
            listOf("--release", "21", "-classpath", classpath, "-d", destination.toString()) +
                sources.map(Path::toString)
        val status = ToolProvider.getSystemJavaCompiler().run(null, null, errors, *arguments.toTypedArray())
        check(status == 0) { "javac failed:\n${errors.toString(Charsets.UTF_8)}" }
    }

    private fun compileKotlin(
        sources: List<Path>,
        destination: Path,
        classpath: String,
    ) {
        val errors = ByteArrayOutputStream()
        val compiler = K2JVMCompiler()
        val arguments =
            compiler.createArguments().apply {
                freeArgs = sources.map(Path::toString)
                this.destination = destination.toString()
                this.classpath = classpath
                jvmTarget = "21"
                noStdlib = true
                noReflect = true
                allWarningsAsErrors = true
            }
        val status =
            compiler.exec(
                PrintingMessageCollector(PrintStream(errors), MessageRenderer.PLAIN_FULL_PATHS, true),
                Services.EMPTY,
                arguments,
            )
        assertEquals(ExitCode.OK, status, "kotlinc failed:\n${errors.toString(Charsets.UTF_8)}")
    }
}
