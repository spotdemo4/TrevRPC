package zip.trev.trevrpc.generator

import com.google.protobuf.DescriptorProtos.DescriptorProto
import com.google.protobuf.DescriptorProtos.FileDescriptorProto
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse
import java.io.InputStream
import java.io.OutputStream
import kotlin.system.exitProcess

private const val DEFAULT_RUNTIME_PACKAGE = "zip.trev.trevrpc"
private const val DEFAULT_FILE_SUFFIX = ".trevrpc.kt"

public fun main() {
    try {
        runPlugin(System.`in`, System.out)
    } catch (error: Throwable) {
        System.err.println("protoc-gen-trevrpc-kotlin: ${error.message ?: error.javaClass.name}")
        exitProcess(1)
    }
}

internal fun runPlugin(
    input: InputStream,
    output: OutputStream,
) {
    generate(CodeGeneratorRequest.parseFrom(input)).writeTo(output)
}

internal fun generate(request: CodeGeneratorRequest): CodeGeneratorResponse {
    val options =
        try {
            PluginOptions.parse(request.parameter)
        } catch (error: IllegalArgumentException) {
            return responseBuilder().setError(error.message.orEmpty()).build()
        }

    val filesToGenerate = request.fileToGenerateList.toSet()
    val types = TypeIndex(request.protoFileList)
    val response = responseBuilder()

    for (file in request.protoFileList) {
        if (file.name !in filesToGenerate || file.serviceCount == 0) continue
        try {
            response.addFile(
                CodeGeneratorResponse.File
                    .newBuilder()
                    .setName(outputFileName(file.name, options.fileSuffix))
                    .setContent(generateFile(file, types, options))
                    .build(),
            )
        } catch (error: GeneratorException) {
            return responseBuilder().setError(error.message.orEmpty()).build()
        }
    }
    return response.build()
}

private fun responseBuilder(): CodeGeneratorResponse.Builder =
    CodeGeneratorResponse
        .newBuilder()
        .setSupportedFeatures(CodeGeneratorResponse.Feature.FEATURE_PROTO3_OPTIONAL_VALUE.toLong())

private data class PluginOptions(
    val runtimePackage: String = DEFAULT_RUNTIME_PACKAGE,
    val fileSuffix: String = DEFAULT_FILE_SUFFIX,
) {
    companion object {
        fun parse(parameter: String): PluginOptions {
            var options = PluginOptions()
            if (parameter.isEmpty()) return options
            for (entry in parameter.split(',').filter(String::isNotEmpty)) {
                val separator = entry.indexOf('=')
                require(separator >= 0) {
                    "invalid trevrpc-kotlin option ${entry.quote()}; expected key=value"
                }
                val key = entry.substring(0, separator)
                val value = entry.substring(separator + 1)
                options =
                    when (key) {
                        "runtime_package" -> {
                            require(value.isNotEmpty()) { "runtime_package must not be empty" }
                            options.copy(runtimePackage = value)
                        }

                        "file_suffix" -> {
                            options.copy(fileSuffix = value)
                        }

                        else -> {
                            throw IllegalArgumentException("unknown trevrpc-kotlin option ${key.quote()}")
                        }
                    }
            }
            return options
        }
    }
}

private data class TypeRef(
    val protoName: String,
    val kotlinType: String,
)

private class TypeIndex(
    files: List<FileDescriptorProto>,
) {
    private val messages = mutableMapOf<String, TypeRef>()

    init {
        for (file in files) {
            val javaPrefix = javaTypePrefix(file)
            for (message in file.messageTypeList) {
                indexMessage(file.`package`, emptyList(), emptyList(), javaPrefix, message)
            }
        }
    }

    fun resolve(
        currentFile: FileDescriptorProto,
        protoName: String,
    ): TypeRef {
        val absoluteName =
            if (protoName.startsWith('.')) {
                protoName
            } else {
                fullProtoName(currentFile.`package`, listOf(protoName))
            }
        return messages[absoluteName]
            ?: throw GeneratorException("unknown protobuf message type ${absoluteName.quote()}")
    }

    private fun indexMessage(
        protoPackage: String,
        protoParents: List<String>,
        javaParents: List<String>,
        javaPrefix: List<String>,
        message: DescriptorProto,
    ) {
        val protoParts = protoParents + message.name
        val javaParts = javaParents + javaClassName(message.name)
        val protoName = fullProtoName(protoPackage, protoParts)
        messages[protoName] = TypeRef(protoName, (javaPrefix + javaParts).joinToString(".", transform = ::kotlinIdent))
        for (nested in message.nestedTypeList) {
            indexMessage(protoPackage, protoParts, javaParts, javaPrefix, nested)
        }
    }
}

private data class Method(
    val name: String,
    val constantName: String,
    val protoName: String,
    val input: TypeRef,
    val output: TypeRef,
    val clientStreaming: Boolean,
    val serverStreaming: Boolean,
) {
    val kotlinName: String
        get() = kotlinIdent(name)

    fun suffixedName(suffix: String): String = kotlinIdent(name + suffix)

    val kind: String
        get() =
            when {
                clientStreaming && serverStreaming -> "BIDIRECTIONAL_STREAMING"
                clientStreaming -> "CLIENT_STREAMING"
                serverStreaming -> "SERVER_STREAMING"
                else -> "UNARY"
            }
}

private data class Service(
    val name: String,
    val interfaceName: String,
    val clientName: String,
    val registerName: String,
    val constantPrefix: String,
    val fullProtoName: String,
    val methods: List<Method>,
)

private fun generateFile(
    file: FileDescriptorProto,
    types: TypeIndex,
    options: PluginOptions,
): String {
    val runtime = options.runtimePackage.split('.').joinToString(".", transform = ::kotlinIdent)
    val services = file.serviceList.map { describeService(file, it, types) }
    val codecs = linkedMapOf<String, TypeRef>()
    for (service in services) {
        for (method in service.methods) {
            codecs.putIfAbsent(method.input.protoName, method.input)
            codecs.putIfAbsent(method.output.protoName, method.output)
        }
    }
    val codecNames = codecs.keys.withIndex().associate { (index, name) -> name to "trevrpcCodec$index" }
    return buildString {
        appendLine("// Code generated by protoc-gen-trevrpc-kotlin. DO NOT EDIT.")
        if (javaPackage(file).isNotEmpty()) {
            appendLine()
            appendLine("package ${javaPackage(file).split('.').joinToString(".", transform = ::kotlinIdent)}")
        }
        appendLine()
        appendLine("import kotlinx.coroutines.coroutineScope")
        appendLine("import kotlinx.coroutines.flow.Flow")
        appendLine("import kotlinx.coroutines.flow.flow")
        appendLine("import kotlinx.coroutines.flow.map")
        appendLine("import kotlinx.coroutines.launch")
        appendLine()
        codecs.values.forEach { type -> generateCodec(runtime, type, checkNotNull(codecNames[type.protoName])) }
        appendLine("private fun <T> decodeTrevrpcRequest(codec: $runtime.MessageCodec<T>, body: ByteArray): T =")
        appendLine("    try {")
        appendLine("        codec.decode(body)")
        appendLine("    } catch (error: $runtime.TrevRpcException) {")
        appendLine("        throw error")
        appendLine("    } catch (error: Throwable) {")
        appendLine("        throw $runtime.TrevRpcException(")
        appendLine(
            "            $runtime.Status.invalidArgument(\"failed to decode request: \${error.message ?: error.javaClass.simpleName}\"),",
        )
        appendLine("            error,")
        appendLine("        )")
        appendLine("    }")
        appendLine()
        appendLine("private fun requireTrevrpcKind(")
        appendLine("    context: $runtime.RequestContext,")
        appendLine("    expected: $runtime.RpcKind,")
        appendLine(") {")
        appendLine("    if (context.kind != expected) {")
        appendLine("        throw $runtime.TrevRpcException(")
        appendLine("            $runtime.Status.invalidArgument(\"RPC kind mismatch: expected \$expected, got \${context.kind}\"),")
        appendLine("        )")
        appendLine("    }")
        appendLine("}")
        appendLine()
        services.forEachIndexed { index, service ->
            if (index > 0) appendLine()
            generateService(runtime, service, codecNames)
        }
    }
}

private fun StringBuilder.generateCodec(
    runtime: String,
    type: TypeRef,
    name: String,
) {
    appendLine("private val $name: $runtime.MessageCodec<${type.kotlinType}> =")
    appendLine("    $runtime.MessageCodec(")
    appendLine("        encode = { message -> message.toByteArray() },")
    appendLine("        decode = { body -> ${type.kotlinType}.parseFrom(body) },")
    appendLine("    )")
    appendLine()
}

private fun describeService(
    file: FileDescriptorProto,
    descriptor: ServiceDescriptorProto,
    types: TypeIndex,
): Service {
    val typeName = javaClassName(descriptor.name)
    val methodNames = mutableSetOf<String>()
    val methods =
        descriptor.methodList.mapIndexed { index, method ->
            describeMethod(file, method, index, methodNames, types)
        }
    return Service(
        name = typeName,
        interfaceName = "${typeName}Service",
        clientName = "${typeName}Client",
        registerName = kotlinIdent("register$typeName"),
        constantPrefix = screamingSnake(descriptor.name),
        fullProtoName = listOf(file.`package`, descriptor.name).filter(String::isNotEmpty).joinToString("."),
        methods = methods,
    )
}

private fun describeMethod(
    file: FileDescriptorProto,
    descriptor: MethodDescriptorProto,
    index: Int,
    usedNames: MutableSet<String>,
    types: TypeIndex,
): Method {
    val baseName = lowerCamel(descriptor.name)
    var name = baseName
    if (!usedNames.add(name)) {
        name = "${baseName}_${index + 1}"
        usedNames.add(name)
    }
    return Method(
        name = name,
        constantName = screamingSnake(descriptor.name),
        protoName = descriptor.name,
        input = types.resolve(file, descriptor.inputType),
        output = types.resolve(file, descriptor.outputType),
        clientStreaming = descriptor.clientStreaming,
        serverStreaming = descriptor.serverStreaming,
    )
}

private fun StringBuilder.generateService(
    runtime: String,
    service: Service,
    codecNames: Map<String, String>,
) {
    val serviceConstant = "${service.constantPrefix}_SERVICE_NAME"
    appendLine("public const val $serviceConstant: String = ${service.fullProtoName.quote()}")
    for (method in service.methods) {
        appendLine(
            "public const val ${service.constantPrefix}_${method.constantName}_METHOD_NAME: String = ${method.protoName.quote()}",
        )
        appendLine(
            "public val ${service.constantPrefix}_${method.constantName}_RPC_KIND: $runtime.RpcKind = $runtime.RpcKind.${method.kind}",
        )
    }
    appendLine()
    appendLine("public interface ${kotlinIdent(service.interfaceName)} {")
    for (method in service.methods) generateServiceMethod(runtime, method)
    appendLine("}")
    appendLine()
    generateClient(runtime, service, serviceConstant, codecNames)
    appendLine()
    generateRegistration(runtime, service, serviceConstant, codecNames)
}

private fun StringBuilder.generateServiceMethod(
    runtime: String,
    method: Method,
) {
    val requestType = if (method.clientStreaming) "Flow<${method.input.kotlinType}>" else method.input.kotlinType
    val responseType = if (method.serverStreaming) "Flow<${method.output.kotlinType}>" else method.output.kotlinType
    appendLine("    public suspend fun ${method.kotlinName}(")
    appendLine("        context: $runtime.RequestContext,")
    appendLine("        ${if (method.clientStreaming) "requests" else "request"}: $requestType,")
    appendLine("    ): $responseType")
    appendLine()
}

private fun StringBuilder.generateClient(
    runtime: String,
    service: Service,
    serviceConstant: String,
    codecNames: Map<String, String>,
) {
    appendLine("public class ${kotlinIdent(service.clientName)}(")
    appendLine("    transport: $runtime.RpcTransport,")
    appendLine("    public val defaultCallOptions: $runtime.CallOptions = $runtime.CallOptions(),")
    appendLine(") {")
    appendLine("    private val client: $runtime.Client = $runtime.Client(transport)")
    appendLine()
    for (method in service.methods) {
        generateClientMethod(runtime, service, method, serviceConstant, codecNames)
    }
    appendLine("}")
}

private fun StringBuilder.generateClientMethod(
    runtime: String,
    service: Service,
    method: Method,
    serviceConstant: String,
    codecNames: Map<String, String>,
) {
    val methodConstant = "${service.constantPrefix}_${method.constantName}_METHOD_NAME"
    val inputCodec = checkNotNull(codecNames[method.input.protoName])
    val outputCodec = checkNotNull(codecNames[method.output.protoName])
    when {
        method.clientStreaming && method.serverStreaming -> {
            appendLine("    public suspend fun ${method.kotlinName}(")
            appendLine("        options: $runtime.CallOptions = defaultCallOptions,")
            appendLine("    ): $runtime.BidirectionalStreamingCall<${method.input.kotlinType}, ${method.output.kotlinType}> =")
            appendLine("        client.bidirectionalStreaming($serviceConstant, $methodConstant, $inputCodec, $outputCodec, options)")
            appendLine()
            appendLine("    public suspend fun ${method.suffixedName("Call")}(")
            appendLine("        options: $runtime.CallOptions = defaultCallOptions,")
            appendLine("    ): $runtime.BidirectionalStreamingCall<${method.input.kotlinType}, ${method.output.kotlinType}> =")
            appendLine("        ${method.kotlinName}(options)")
            appendLine()
            appendLine("    public fun ${method.kotlinName}(")
            appendLine("        requests: Flow<${method.input.kotlinType}>,")
            appendLine("        options: $runtime.CallOptions = defaultCallOptions,")
            appendLine("    ): Flow<${method.output.kotlinType}> =")
            appendLine("        flow {")
            appendLine("            coroutineScope {")
            appendLine("                val call = ${method.suffixedName("Call")}(options)")
            appendLine("                val sender = launch {")
            appendLine("                    requests.collect { request -> call.send(request) }")
            appendLine("                    call.closeSend()")
            appendLine("                }")
            appendLine("                try {")
            appendLine("                    while (true) {")
            appendLine("                        val response = call.receive() ?: break")
            appendLine("                        emit(response)")
            appendLine("                    }")
            appendLine("                } finally {")
            appendLine("                    sender.cancel()")
            appendLine("                    call.close()")
            appendLine("                }")
            appendLine("            }")
            appendLine("        }")
        }

        method.clientStreaming -> {
            appendLine("    public suspend fun ${method.kotlinName}(")
            appendLine("        options: $runtime.CallOptions = defaultCallOptions,")
            appendLine("    ): $runtime.ClientStreamingCall<${method.input.kotlinType}, ${method.output.kotlinType}> =")
            appendLine("        client.clientStreaming($serviceConstant, $methodConstant, $inputCodec, $outputCodec, options)")
            appendLine()
            appendLine("    public suspend fun ${method.suffixedName("Call")}(")
            appendLine("        options: $runtime.CallOptions = defaultCallOptions,")
            appendLine("    ): $runtime.ClientStreamingCall<${method.input.kotlinType}, ${method.output.kotlinType}> =")
            appendLine("        ${method.kotlinName}(options)")
            appendLine()
            appendLine("    public suspend fun ${method.suffixedName("Envelope")}(")
            appendLine("        requests: Flow<${method.input.kotlinType}>,")
            appendLine("        options: $runtime.CallOptions = defaultCallOptions,")
            appendLine("    ): $runtime.ResponseEnvelope<${method.output.kotlinType}> {")
            appendLine("        val call = ${method.suffixedName("Call")}(options)")
            appendLine("        return try {")
            appendLine("            requests.collect { request -> call.send(request) }")
            appendLine("            call.closeSend()")
            appendLine("            call.receive()")
            appendLine("        } finally {")
            appendLine("            call.close()")
            appendLine("        }")
            appendLine("    }")
            appendLine()
            appendLine("    public suspend fun ${method.kotlinName}(")
            appendLine("        requests: Flow<${method.input.kotlinType}>,")
            appendLine("        options: $runtime.CallOptions = defaultCallOptions,")
            appendLine("    ): ${method.output.kotlinType} = ${method.suffixedName("Envelope")}(requests, options).message")
        }

        method.serverStreaming -> {
            appendLine("    public suspend fun ${method.suffixedName("Call")}(")
            appendLine("        request: ${method.input.kotlinType},")
            appendLine("        options: $runtime.CallOptions = defaultCallOptions,")
            appendLine("    ): $runtime.ServerStreamingCall<${method.output.kotlinType}> =")
            appendLine("        client.serverStreaming($serviceConstant, $methodConstant, request, $inputCodec, $outputCodec, options)")
            appendLine()
            appendLine("    public fun ${method.kotlinName}(")
            appendLine("        request: ${method.input.kotlinType},")
            appendLine("        options: $runtime.CallOptions = defaultCallOptions,")
            appendLine("    ): Flow<${method.output.kotlinType}> =")
            appendLine("        flow {")
            appendLine("            val call = ${method.suffixedName("Call")}(request, options)")
            appendLine("            try {")
            appendLine("                while (true) {")
            appendLine("                    val response = call.receive() ?: break")
            appendLine("                    emit(response)")
            appendLine("                }")
            appendLine("            } finally {")
            appendLine("                call.close()")
            appendLine("            }")
            appendLine("        }")
        }

        else -> {
            appendLine("    public suspend fun ${method.suffixedName("Envelope")}(")
            appendLine("        request: ${method.input.kotlinType},")
            appendLine("        options: $runtime.CallOptions = defaultCallOptions,")
            appendLine("    ): $runtime.ResponseEnvelope<${method.output.kotlinType}> =")
            appendLine("        client.unaryEnvelope($serviceConstant, $methodConstant, request, $inputCodec, $outputCodec, options)")
            appendLine()
            appendLine("    public suspend fun ${method.kotlinName}(")
            appendLine("        request: ${method.input.kotlinType},")
            appendLine("        options: $runtime.CallOptions = defaultCallOptions,")
            appendLine("    ): ${method.output.kotlinType} = ${method.suffixedName("Envelope")}(request, options).message")
        }
    }
    appendLine()
}

private fun StringBuilder.generateRegistration(
    runtime: String,
    service: Service,
    serviceConstant: String,
    codecNames: Map<String, String>,
) {
    appendLine("public fun ${service.registerName}(")
    appendLine("    server: $runtime.Server,")
    appendLine("    service: ${kotlinIdent(service.interfaceName)},")
    appendLine(") {")
    for (method in service.methods) {
        val methodConstant = "${service.constantPrefix}_${method.constantName}_METHOD_NAME"
        val kindConstant = "${service.constantPrefix}_${method.constantName}_RPC_KIND"
        val inputCodec = checkNotNull(codecNames[method.input.protoName])
        val outputCodec = checkNotNull(codecNames[method.output.protoName])
        when {
            method.clientStreaming && method.serverStreaming -> {
                appendLine("    server.routeBidirectionalStreaming($serviceConstant, $methodConstant) { context, requests ->")
                appendLine("        requireTrevrpcKind(context, $kindConstant)")
                appendLine("        val decoded = requests.map { body -> decodeTrevrpcRequest($inputCodec, body) }")
                appendLine("        $runtime.ResponseEnvelope(service.${method.kotlinName}(context, decoded).map($outputCodec.encode))")
                appendLine("    }")
            }

            method.clientStreaming -> {
                appendLine("    server.routeClientStreaming($serviceConstant, $methodConstant) { context, requests ->")
                appendLine("        requireTrevrpcKind(context, $kindConstant)")
                appendLine("        val decoded = requests.map { body -> decodeTrevrpcRequest($inputCodec, body) }")
                appendLine("        $runtime.ResponseEnvelope($outputCodec.encode(service.${method.kotlinName}(context, decoded)))")
                appendLine("    }")
            }

            method.serverStreaming -> {
                appendLine("    server.routeServerStreaming($serviceConstant, $methodConstant) { context, body ->")
                appendLine("        requireTrevrpcKind(context, $kindConstant)")
                appendLine("        val request = decodeTrevrpcRequest($inputCodec, body)")
                appendLine("        $runtime.ResponseEnvelope(service.${method.kotlinName}(context, request).map($outputCodec.encode))")
                appendLine("    }")
            }

            else -> {
                appendLine("    server.routeUnary($serviceConstant, $methodConstant) { context, body ->")
                appendLine("        requireTrevrpcKind(context, $kindConstant)")
                appendLine("        val request = decodeTrevrpcRequest($inputCodec, body)")
                appendLine("        $runtime.ResponseEnvelope($outputCodec.encode(service.${method.kotlinName}(context, request)))")
                appendLine("    }")
            }
        }
    }
    appendLine("}")
}

private fun javaTypePrefix(file: FileDescriptorProto): List<String> {
    val prefix = javaPackage(file).split('.').filter(String::isNotEmpty)
    return if (file.options.javaMultipleFiles) prefix else prefix + javaOuterClassName(file)
}

private fun javaPackage(file: FileDescriptorProto): String = if (file.options.hasJavaPackage()) file.options.javaPackage else file.`package`

private fun javaOuterClassName(file: FileDescriptorProto): String {
    if (file.options.hasJavaOuterClassname()) return file.options.javaOuterClassname
    val basename = file.name.substringAfterLast('/').substringBeforeLast('.', file.name.substringAfterLast('/'))
    var candidate = javaClassName(basename)
    val topLevelNames =
        buildSet {
            file.messageTypeList.mapTo(this) { javaClassName(it.name) }
            file.enumTypeList.mapTo(this) { javaClassName(it.name) }
            file.serviceList.mapTo(this) { javaClassName(it.name) }
        }
    while (candidate in topLevelNames) candidate += "OuterClass"
    return candidate
}

private fun javaClassName(name: String): String = camelCase(name, upperFirst = true)

private fun lowerCamel(name: String): String = camelCase(name, upperFirst = false)

private fun camelCase(
    input: String,
    upperFirst: Boolean,
): String {
    val output = StringBuilder(input.length)
    var capitalizeNext = upperFirst
    input.forEachIndexed { index, character ->
        when {
            character == '_' || character == '-' || character == '.' -> {
                capitalizeNext = true
            }

            character.isDigit() -> {
                output.append(character)
                capitalizeNext = true
            }

            capitalizeNext -> {
                output.append(character.uppercaseChar())
                capitalizeNext = false
            }

            index == 0 && !upperFirst -> {
                output.append(character.lowercaseChar())
            }

            else -> {
                output.append(character)
            }
        }
    }
    return output.toString().ifEmpty { "_" }
}

private fun screamingSnake(input: String): String {
    val output = StringBuilder(input.length + 8)
    input.forEachIndexed { index, character ->
        if (character.isUpperCase() && index > 0 && input[index - 1] != '_') output.append('_')
        if (character == '-' || character == '.') output.append('_') else output.append(character.uppercaseChar())
    }
    return output.toString().ifEmpty { "_" }
}

private fun kotlinIdent(identifier: String): String =
    if (identifier in KOTLIN_KEYWORDS || !identifier.isKotlinIdentifier()) "`$identifier`" else identifier

private fun String.isKotlinIdentifier(): Boolean =
    isNotEmpty() && (first() == '_' || first().isLetter()) && drop(1).all { it == '_' || it.isLetterOrDigit() }

private fun fullProtoName(
    protoPackage: String,
    names: List<String>,
): String = "." + (listOf(protoPackage).filter(String::isNotEmpty) + names).joinToString(".")

private fun outputFileName(
    input: String,
    suffix: String,
): String {
    val slash = input.lastIndexOf('/')
    val dot = input.lastIndexOf('.')
    val extensionStart = if (dot > slash) dot else input.length
    return input.substring(0, extensionStart) + suffix
}

private fun String.quote(): String =
    buildString {
        append('"')
        for (character in this@quote) {
            when (character) {
                '\\' -> append("\\\\")
                '"' -> append("\\\"")
                '\n' -> append("\\n")
                '\r' -> append("\\r")
                '\t' -> append("\\t")
                '$' -> append("\\$")
                else -> append(character)
            }
        }
        append('"')
    }

private class GeneratorException(
    message: String,
) : RuntimeException(message)

private val KOTLIN_KEYWORDS =
    setOf(
        "as",
        "abstract",
        "actual",
        "annotation",
        "break",
        "by",
        "catch",
        "class",
        "companion",
        "const",
        "constructor",
        "continue",
        "context",
        "crossinline",
        "data",
        "delegate",
        "do",
        "dynamic",
        "else",
        "enum",
        "expect",
        "external",
        "false",
        "field",
        "file",
        "final",
        "finally",
        "for",
        "fun",
        "get",
        "if",
        "import",
        "in",
        "infix",
        "init",
        "inline",
        "inner",
        "interface",
        "internal",
        "is",
        "lateinit",
        "noinline",
        "null",
        "object",
        "open",
        "operator",
        "out",
        "override",
        "package",
        "param",
        "private",
        "property",
        "protected",
        "public",
        "receiver",
        "reified",
        "return",
        "sealed",
        "set",
        "setparam",
        "super",
        "suspend",
        "tailrec",
        "this",
        "throw",
        "true",
        "try",
        "typealias",
        "typeof",
        "val",
        "value",
        "var",
        "vararg",
        "when",
        "where",
        "while",
    )
