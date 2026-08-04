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
import org.junit.jupiter.api.Assertions.assertFalse
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
        val content = response.fileList.single().content
        assertFalse(Regex("fun \\w+(Envelope|Call)\\(").containsMatchIn(content))
        assertEquals(golden("all-shapes.txt"), snapshot(content))
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
    fun `permits normalized RPC overloads with distinct source JVM and constant signatures`() {
        val file =
            collisionFile(
                "api/normalized.proto",
                ServiceDescriptorProto
                    .newBuilder()
                    .setName("Names")
                    .addMethod(method("Foo_Bar", ".collision.Request", ".collision.Reply"))
                    .addMethod(method("Foo__Bar", ".collision.OtherRequest", ".collision.Reply"))
                    .build(),
            )

        val response = generate(request(file))

        assertEquals("", response.error)
        val content = response.fileList.single().content
        assertEquals(4, Regex("fun fooBar\\(").findAll(content).count())
        assertTrue(content.contains("NAMES_FOO_BAR_METHOD_NAME"))
        assertTrue(content.contains("NAMES_FOO__BAR_METHOD_NAME"))
    }

    @Test
    fun `rejects actual normalized source signature collisions`() {
        val file =
            collisionFile(
                "api/normalized-source.proto",
                ServiceDescriptorProto
                    .newBuilder()
                    .setName("Names")
                    .addMethod(method("Foo_Bar", ".collision.Request", ".collision.Reply"))
                    .addMethod(method("Foo__Bar", ".collision.Request", ".collision.Reply"))
                    .build(),
            )

        assertDeterministicCollision(
            request(file),
            "fun collision.generated.NamesClient.fooBar(collision.generated.Request, zip.trev.trevrpc.CallOptions)",
            "collision.Names.Foo_Bar",
            "collision.Names.Foo__Bar",
        )
    }

    @Test
    fun `rejects normalized client alias collisions`() {
        val file =
            collisionFile(
                "api/normalized-erasure.proto",
                ServiceDescriptorProto
                    .newBuilder()
                    .setName("Names")
                    .addMethod(
                        method("Foo_Bar", ".collision.Request", ".collision.Reply")
                            .setClientStreaming(true),
                    ).addMethod(
                        method("Foo__Bar", ".collision.OtherRequest", ".collision.Reply")
                            .setClientStreaming(true),
                    ).build(),
            )

        assertDeterministicCollision(
            request(file),
            "fun collision.generated.NamesClient.fooBar(zip.trev.trevrpc.CallOptions)",
            "collision.Names.Foo_Bar",
            "collision.Names.Foo__Bar",
        )
    }

    @Test
    fun `rejects Response suffix and JVM erased signature collisions`() {
        val file =
            collisionFile(
                "api/response.proto",
                ServiceDescriptorProto
                    .newBuilder()
                    .setName("Names")
                    .addMethod(method("Foo", ".collision.Request", ".collision.Reply"))
                    .addMethod(method("FooResponse", ".collision.Request", ".collision.Reply"))
                    .build(),
            )

        assertDeterministicCollision(
            request(file),
            "fun collision.generated.NamesClient.fooResponse(collision.generated.Request, zip.trev.trevrpc.CallOptions)",
            "collision.Names.Foo",
            "collision.Names.FooResponse",
        )
    }

    @Test
    fun `rejects duplicate method constant stems across services`() {
        val file =
            collisionFile(
                "api/constants.proto",
                ServiceDescriptorProto
                    .newBuilder()
                    .setName("A")
                    .addMethod(method("B_C", ".collision.Request", ".collision.Reply"))
                    .build(),
                ServiceDescriptorProto
                    .newBuilder()
                    .setName("A_B")
                    .addMethod(method("C", ".collision.Request", ".collision.Reply"))
                    .build(),
            )

        assertDeterministicCollision(
            request(file),
            "val collision.generated.A_B_C_METHOD_NAME",
            "collision.A.B_C",
            "collision.A_B.C",
        )
    }

    @Test
    fun `rejects generated service types that collide with protobuf Java types`() {
        val imported =
            FileDescriptorProto
                .newBuilder()
                .setName("api/types.proto")
                .setPackage("types")
                .setOptions(javaOptions())
                .addMessageType(message("GreeterClient"))
                .build()
        val target =
            collisionFile(
                "api/service.proto",
                ServiceDescriptorProto
                    .newBuilder()
                    .setName("Greeter")
                    .addMethod(method("Call", ".collision.Request", ".collision.Reply"))
                    .build(),
            )

        assertDeterministicCollision(
            request(target, additionalFiles = listOf(imported)),
            "class collision.generated.GreeterClient",
            "collision.Greeter",
            "types.GreeterClient",
        )
    }

    @Test
    fun `preserves protoc Java message and nested identifiers exactly`() {
        val nested = message("outer_name").toBuilder().addNestedType(message("_nested_Name")).build()
        val file =
            FileDescriptorProto
                .newBuilder()
                .setName("api/names.proto")
                .setPackage("collision")
                .setOptions(javaOptions())
                .addMessageType(nested)
                .addMessageType(message("_leading_Name"))
                .addService(
                    ServiceDescriptorProto
                        .newBuilder()
                        .setName("Names")
                        .addMethod(method("Nested", ".collision.outer_name._nested_Name", ".collision._leading_Name")),
                ).build()

        val response = generate(request(file))

        assertEquals("", response.error)
        val content = response.fileList.single().content
        assertTrue(content.contains("collision.generated.outer_name._nested_Name"), content)
        assertTrue(content.contains("collision.generated._leading_Name"), content)
        assertFalse(content.contains("OuterName"), content)
        assertFalse(content.contains("NestedName"), content)
    }

    @Test
    fun `reserves descriptor outer class with java multiple files enabled`() {
        val file =
            collisionFile(
                "api/names_client.proto",
                ServiceDescriptorProto
                    .newBuilder()
                    .setName("Names")
                    .addMethod(method("Call", ".collision.Request", ".collision.Reply"))
                    .build(),
            )

        assertDeterministicCollision(
            request(file),
            "class collision.generated.NamesClient",
            "collision.Names",
            "protobuf descriptor for file api/names_client.proto",
        )
    }

    @Test
    fun `rejects JVM file facade collisions with protobuf Java types`() {
        val fileName = "facade/collision.proto"
        val facadeName = "TrevRpcGenerated_b25e2788e91b89499ff67826209b03cebb02b7f8e31f9560d1aa93920ae386dc"
        val file =
            FileDescriptorProto
                .newBuilder()
                .setName(fileName)
                .setPackage("collision")
                .setOptions(
                    FileOptions
                        .newBuilder()
                        .setJavaPackage("collision.generated")
                        .setJavaOuterClassname(facadeName),
                ).addMessageType(message("Request"))
                .addMessageType(message("Reply"))
                .addService(
                    ServiceDescriptorProto
                        .newBuilder()
                        .setName("Api")
                        .addMethod(method("Call", ".collision.Request", ".collision.Reply")),
                ).build()

        assertDeterministicCollision(
            request(file),
            "class collision.generated.$facadeName",
            "protobuf descriptor for file $fileName",
            "protobuf file $fileName",
        )
    }

    @Test
    fun `assigns deterministic full path JVM file facades`() {
        val first =
            FileDescriptorProto
                .newBuilder()
                .setName("left/shared.proto")
                .setPackage("left")
                .setOptions(javaOptions().toBuilder().setJavaOuterClassname("LeftShared"))
                .addMessageType(message("LeftRequest"))
                .addMessageType(message("LeftReply"))
                .addService(
                    ServiceDescriptorProto
                        .newBuilder()
                        .setName("Left")
                        .addMethod(method("Call", ".left.LeftRequest", ".left.LeftReply")),
                ).build()
        val second =
            FileDescriptorProto
                .newBuilder()
                .setName("right/shared.proto")
                .setPackage("right")
                .setOptions(javaOptions().toBuilder().setJavaOuterClassname("RightShared"))
                .addMessageType(message("RightRequest"))
                .addMessageType(message("RightReply"))
                .addService(
                    ServiceDescriptorProto
                        .newBuilder()
                        .setName("Right")
                        .addMethod(method("Call", ".right.RightRequest", ".right.RightReply")),
                ).build()

        val response = generate(request(first, second))

        assertEquals("", response.error)
        val annotations = response.fileList.map { file -> file.content.lineSequence().first { "@file:JvmName" in it } }
        assertEquals(
            listOf(
                "@file:JvmName(\"TrevRpcGenerated_a09d84e63935036571c5b718c608f66f5f60ded6ecb4897f5900b8ce820872aa\")",
                "@file:JvmName(\"TrevRpcGenerated_7f74987fd15041d30f5b4a4d86f9d46e3fa5b17aa5962b287cf4cb603e7c441b\")",
            ),
            annotations,
        )
        assertEquals(
            annotations,
            generate(request(first, second)).fileList.map {
                it.content.lineSequence().first { line ->
                    "@file:JvmName" in line
                }
            },
        )
    }

    @Test
    fun `rejects duplicate generated output names`() {
        val first =
            collisionFile(
                "api/same.proto",
                ServiceDescriptorProto
                    .newBuilder()
                    .setName("First")
                    .addMethod(method("Call", ".collision.Request", ".collision.Reply"))
                    .build(),
                protoPackage = "collision",
                javaPackage = "first.generated",
            )
        val second =
            collisionFile(
                "api/same.rpc",
                ServiceDescriptorProto
                    .newBuilder()
                    .setName("Second")
                    .addMethod(method("Call", ".other.Request", ".other.Reply"))
                    .build(),
                protoPackage = "other",
                javaPackage = "second.generated",
            )

        assertDeterministicCollision(
            request(first, second),
            "output file api/same.trevrpc.kt",
            "protobuf file api/same.proto",
            "protobuf file api/same.rpc",
        )
    }

    @Test
    fun `rejects cross file declarations sharing a Java package`() {
        val first =
            collisionFile(
                "api/first.proto",
                ServiceDescriptorProto
                    .newBuilder()
                    .setName("FooBar")
                    .addMethod(method("Call", ".first.Request", ".first.Reply"))
                    .build(),
                protoPackage = "first",
            )
        val second =
            collisionFile(
                "api/second.proto",
                ServiceDescriptorProto
                    .newBuilder()
                    .setName("Foo_Bar")
                    .addMethod(method("Call", ".second.Request", ".second.Reply"))
                    .build(),
                protoPackage = "second",
            )

        assertDeterministicCollision(
            request(first, second),
            "fun collision.generated.registerFooBar(Server, FooBarService)",
            "first.FooBar",
            "second.Foo_Bar",
        )
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

    private fun assertDeterministicCollision(
        request: CodeGeneratorRequest,
        declaration: String,
        vararg symbols: String,
    ) {
        val expected = generate(request).error
        assertTrue(expected.contains(declaration), expected)
        symbols.forEach { symbol -> assertTrue(expected.contains(symbol), expected) }
        assertTrue(generate(request).fileList.isEmpty())
        assertEquals(expected, generate(reverseDescriptors(request)).error)
    }

    private fun reverseDescriptors(request: CodeGeneratorRequest): CodeGeneratorRequest {
        val files =
            request.protoFileList.reversed().map { file ->
                val services =
                    file.serviceList.reversed().map { service ->
                        service
                            .toBuilder()
                            .clearMethod()
                            .addAllMethod(service.methodList.reversed())
                            .build()
                    }
                file
                    .toBuilder()
                    .clearService()
                    .addAllService(services)
                    .build()
            }
        return request
            .toBuilder()
            .clearProtoFile()
            .addAllProtoFile(files)
            .build()
    }

    private fun request(
        first: FileDescriptorProto,
        second: FileDescriptorProto? = null,
        additionalFiles: List<FileDescriptorProto> = emptyList(),
    ): CodeGeneratorRequest {
        val generated = listOfNotNull(first, second)
        return CodeGeneratorRequest
            .newBuilder()
            .addAllFileToGenerate(generated.map(FileDescriptorProto::getName))
            .addAllProtoFile(additionalFiles + generated)
            .build()
    }

    private fun collisionFile(
        name: String,
        vararg services: ServiceDescriptorProto,
        protoPackage: String = "collision",
        javaPackage: String = "collision.generated",
    ): FileDescriptorProto =
        FileDescriptorProto
            .newBuilder()
            .setName(name)
            .setPackage(protoPackage)
            .setOptions(javaOptions(javaPackage))
            .addMessageType(message("Request"))
            .addMessageType(message("OtherRequest"))
            .addMessageType(message("Reply"))
            .addAllService(services.toList())
            .build()

    private fun javaOptions(javaPackage: String = "collision.generated"): FileOptions =
        FileOptions
            .newBuilder()
            .setJavaPackage(javaPackage)
            .setJavaMultipleFiles(true)
            .build()

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
                "decodeProtobuf(",
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
        compileSchemas(
            temp,
            schema.parent,
            listOf(schema.fileName.toString()),
            mapOf("CompileFixture.kt" to COMPILE_FIXTURE),
        )
    }

    @Test
    fun `protoc javac and kotlinc accept adversarial names file facades and legal overloads`(
        @TempDir temp: Path,
    ) {
        val fixturePath = "trevrpc-kotlin/protoc-gen-trevrpc-kotlin/src/test/proto/compat"
        val fixtureRoot =
            generateSequence(Path.of(System.getProperty("user.dir")).toAbsolutePath(), Path::getParent)
                .map { it.resolve(fixturePath) }
                .firstOrNull { it.resolve("overloads.proto").isRegularFile() }
                ?: error("generator compatibility fixtures not found from ${System.getProperty("user.dir")}")

        compileSchemas(
            temp,
            fixtureRoot,
            listOf(
                "naming/single_file.proto",
                "naming/multiple_files.proto",
                "naming/_mixed_CASE_file.proto",
                "naming/custom_extension.rpc",
                "naming/trailing_hash#.proto",
                "left/shared.proto",
                "right/shared.proto",
                "overloads.proto",
            ),
            mapOf(
                "NamingFixture.kt" to NAMING_FIXTURE,
                "DerivedFixture.kt" to DERIVED_FIXTURE,
                "SharedFixture.kt" to SHARED_FIXTURE,
                "OverloadsFixture.kt" to OVERLOADS_FIXTURE,
            ),
        )

        val generated = temp.resolve("kotlin")
        val left = generated.resolve("left/shared.trevrpc.kt").readText()
        val right = generated.resolve("right/shared.trevrpc.kt").readText()
        val leftFacade = left.lineSequence().first { "@file:JvmName" in it }
        val rightFacade = right.lineSequence().first { "@file:JvmName" in it }
        assertTrue(leftFacade.startsWith("@file:JvmName(\"TrevRpcGenerated_"), leftFacade)
        assertTrue(rightFacade.startsWith("@file:JvmName(\"TrevRpcGenerated_"), rightFacade)
        assertFalse(leftFacade == rightFacade)
    }

    private fun compileSchemas(
        temp: Path,
        protoRoot: Path,
        protoFiles: List<String>,
        fixtures: Map<String, String>,
    ) {
        val javaSources = temp.resolve("java").createDirectories()
        val kotlinSources = temp.resolve("kotlin").createDirectories()
        val plugin = Path.of(checkNotNull(System.getProperty("trevrpc.kotlin.plugin")))
        runCommand(
            listOf(
                "protoc",
                "--proto_path=$protoRoot",
                "--java_out=$javaSources",
                "--plugin=protoc-gen-trevrpc-kotlin=$plugin",
                "--trevrpc-kotlin_out=$kotlinSources",
            ) + protoFiles,
            protoRoot,
        )
        fixtures.forEach { (name, content) -> kotlinSources.resolve(name).toFile().writeText(content) }

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

    private companion object {
        val COMPILE_FIXTURE =
            """
            package example.greeter

            import kotlinx.coroutines.flow.Flow
            import kotlinx.coroutines.flow.flowOf
            import zip.trev.trevrpc.BidirectionalStreamingCall
            import zip.trev.trevrpc.ClientStreamingCall
            import zip.trev.trevrpc.RequestContext
            import zip.trev.trevrpc.ResponseEnvelope
            import zip.trev.trevrpc.ServerStreamingCall

            private typealias Request = GreeterOuterClass.HelloRequest
            private typealias Reply = GreeterOuterClass.HelloReply

            private class CompileService : GreeterService {
                override suspend fun sayHello(context: RequestContext, request: Request): ResponseEnvelope<Reply> =
                    ResponseEnvelope(Reply.getDefaultInstance())

                override suspend fun lotsOfReplies(
                    context: RequestContext,
                    request: Request,
                ): ResponseEnvelope<Flow<Reply>> = ResponseEnvelope(flowOf(Reply.getDefaultInstance()))

                override suspend fun lotsOfGreetings(
                    context: RequestContext,
                    requests: Flow<Request>,
                ): ResponseEnvelope<Reply> = ResponseEnvelope(Reply.getDefaultInstance())

                override suspend fun bidiHello(
                    context: RequestContext,
                    requests: Flow<Request>,
                ): ResponseEnvelope<Flow<Reply>> = ResponseEnvelope(flowOf(Reply.getDefaultInstance()))
            }

            @Suppress("UNUSED_VARIABLE")
            private suspend fun typeCheck(client: GreeterClient, request: Request, requests: Flow<Request>) {
                val unaryResponse: ResponseEnvelope<Reply> = client.sayHelloResponse(request)
                val unary: Reply = client.sayHello(request)
                val serverResponse: ServerStreamingCall<Reply> = client.lotsOfRepliesResponse(request)
                val server: Flow<Reply> = client.lotsOfReplies(request)
                val clientResponse: ClientStreamingCall<Request, Reply> = client.lotsOfGreetingsResponse()
                val clientAlias: ClientStreamingCall<Request, Reply> = client.lotsOfGreetings()
                val clientCollectedResponse: ResponseEnvelope<Reply> = client.lotsOfGreetingsResponse(requests)
                val clientCollected: Reply = client.lotsOfGreetings(requests)
                val bidiResponse: BidirectionalStreamingCall<Request, Reply> = client.bidiHelloResponse()
                val bidiAlias: BidirectionalStreamingCall<Request, Reply> = client.bidiHello()
                val bidi: Flow<Reply> = client.bidiHello(requests)
            }
            """.trimIndent()

        val NAMING_FIXTURE =
            """
            package generator.compat

            @Suppress("unused")
            private fun preserveNames(
                singleNested: SingleFileTypes.snake_name._nested_Name,
                singleLeading: SingleFileTypes._leading_Name,
                mixed: SingleFileTypes.mixedCASE_name,
                multipleNested: multi_snake_name._nested_Mixed,
                multipleLeading: _multi_leading,
            ) = listOf(singleNested, singleLeading, mixed, multipleNested, multipleLeading)
            """.trimIndent()

        val DERIVED_FIXTURE =
            """
            package generator.compat.derived

            @Suppress("unused")
            private fun derivedOuterNames(
                request: MixedCASEFile.raw_message_name,
                response: MixedCASEFile._raw_Output,
                customExtension: CustomExtensionRpc.custom_request,
                trailingHash: TrailingHash_.hash_request,
            ) = listOf(request, response, customExtension, trailingHash)
            """.trimIndent()

        val SHARED_FIXTURE =
            """
            package generator.compat.shared

            @Suppress("unused")
            private fun distinctClients(left: LeftApiClient, right: RightApiClient) = left to right
            """.trimIndent()

        val OVERLOADS_FIXTURE =
            """
            package generator.compat.overloads

            import zip.trev.trevrpc.RequestContext
            import zip.trev.trevrpc.ResponseEnvelope

            private class CompileOverloads : LegalOverloadsService {
                override suspend fun fooBar(
                    context: RequestContext,
                    request: FirstRequest,
                ): ResponseEnvelope<Reply> = ResponseEnvelope(Reply.getDefaultInstance())

                override suspend fun fooBar(
                    context: RequestContext,
                    request: SecondRequest,
                ): ResponseEnvelope<Reply> = ResponseEnvelope(Reply.getDefaultInstance())
            }

            @Suppress("unused")
            private suspend fun typeCheckOverloads(
                client: LegalOverloadsClient,
                first: FirstRequest,
                second: SecondRequest,
            ) {
                val firstReply: Reply = client.fooBar(first)
                val secondReply: Reply = client.fooBar(second)
                val firstMethod: String = LEGAL_OVERLOADS_FOO_BAR_METHOD_NAME
                val secondMethod: String = LEGAL_OVERLOADS_FOO__BAR_METHOD_NAME
            }
            """.trimIndent()
    }
}
