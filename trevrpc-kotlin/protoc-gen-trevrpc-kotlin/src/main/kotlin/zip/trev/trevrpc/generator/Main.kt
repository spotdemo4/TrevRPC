package zip.trev.trevrpc.generator

import com.google.protobuf.DescriptorProtos.DescriptorProto
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto
import com.google.protobuf.DescriptorProtos.FileDescriptorProto
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto
import com.google.protobuf.InvalidProtocolBufferException
import com.google.protobuf.UnknownFieldSet
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorRequest
import com.google.protobuf.compiler.PluginProtos.CodeGeneratorResponse
import java.io.InputStream
import java.io.OutputStream
import java.security.MessageDigest
import kotlin.system.exitProcess

private const val DEFAULT_RUNTIME_PACKAGE = "zip.trev.trevrpc"
private const val DEFAULT_FILE_SUFFIX = ".trevrpc.kt"
private const val HEX_DIGITS = "0123456789abcdef"

/** Extension number of `(trevrpc.method)` on `google.protobuf.MethodOptions`, from trevrpc/options.proto. */
private const val METHOD_OPTION_FIELD = 58247
private const val UNRELIABLE_REQUESTS_FIELD = 1
private const val UNRELIABLE_RESPONSES_FIELD = 2
private const val COMPRESS_STREAM_FIELD = 3

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
    try {
        validateDeclarations(request, filesToGenerate, types, options)
    } catch (error: GeneratorException) {
        return responseBuilder().setError(error.message.orEmpty()).build()
    }
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
    private val descriptors = mutableMapOf<String, DescriptorProto>()
    private val redacted = mutableMapOf<String, Boolean>()

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

    /**
     * Whether messages of [type] contain a field marked `debug_redact`, directly or in a message
     * they contain, so that stream compression must leave them out. Recursive types are walked once.
     */
    fun hasRedactedField(type: TypeRef): Boolean = hasRedactedField(type.protoName, mutableSetOf())

    private fun hasRedactedField(
        protoName: String,
        visiting: MutableSet<String>,
    ): Boolean {
        redacted[protoName]?.let { return it }
        val message = descriptors[protoName] ?: return false
        if (!visiting.add(protoName)) return false
        val result =
            message.fieldList.any { field ->
                field.options.debugRedact || (field.typeName.isNotEmpty() && hasRedactedField(field.typeName, visiting))
            }
        visiting.remove(protoName)
        // A type on a cycle is decided by its outermost visit; only that result is cached.
        if (visiting.isEmpty() || result) redacted[protoName] = result
        return result
    }

    private fun indexMessage(
        protoPackage: String,
        protoParents: List<String>,
        javaParents: List<String>,
        javaPrefix: List<String>,
        message: DescriptorProto,
    ) {
        val protoParts = protoParents + message.name
        val javaParts = javaParents + message.name
        val protoName = fullProtoName(protoPackage, protoParts)
        messages[protoName] = TypeRef(protoName, (javaPrefix + javaParts).joinToString(".", transform = ::kotlinIdent))
        descriptors[protoName] = message
        for (nested in message.nestedTypeList) {
            indexMessage(protoPackage, protoParts, javaParts, javaPrefix, nested)
        }
    }
}

private data class DeclarationReservation(
    val key: String,
    val declaration: String,
    val symbol: String,
)

private fun validateDeclarations(
    request: CodeGeneratorRequest,
    filesToGenerate: Set<String>,
    types: TypeIndex,
    options: PluginOptions,
) {
    val reservations = mutableListOf<DeclarationReservation>()

    fun reserve(
        key: String,
        declaration: String,
        symbol: String,
    ) {
        reservations += DeclarationReservation(key, declaration, symbol)
    }

    for (file in request.protoFileList) {
        val packageName = javaPackage(file)
        val renderedPackage = packageName.split('.').filter(String::isNotEmpty).joinToString(".", transform = ::kotlinIdent)
        val packagePrefix = renderedPackage.takeIf(String::isNotEmpty)?.plus('.') ?: ""
        val outerClass = javaOuterClassName(file)
        reserve(
            "type:$packageName:$outerClass",
            "class $packagePrefix${kotlinIdent(outerClass)}",
            "protobuf descriptor for file ${file.name}",
        )
        if (file.options.javaMultipleFiles) {
            file.messageTypeList.forEach { message ->
                reserveJavaType(reservations, packageName, packagePrefix, message, protoSymbol(file, message.name))
            }
            file.enumTypeList.forEach { enum ->
                reserveJavaType(reservations, packageName, packagePrefix, enum, protoSymbol(file, enum.name))
            }
        }

        if (file.name !in filesToGenerate || file.serviceCount == 0) continue
        val outputName = outputFileName(file.name, options.fileSuffix)
        reserve("output:$outputName", "output file $outputName", "protobuf file ${file.name}")
        val fileFacade = kotlinFileFacadeName(file.name)
        reserve(
            "type:$packageName:$fileFacade",
            "JVM file facade $packagePrefix$fileFacade",
            "protobuf file ${file.name}",
        )

        for (descriptor in file.serviceList) {
            val service = describeService(file, descriptor, types)
            val serviceSymbol = service.fullProtoName
            reserveType(packageName, packagePrefix, service.clientName, serviceSymbol, reservations)

            for (method in service.methods) {
                val methodSymbol = "$serviceSymbol.${method.protoName}"
                reserveProperty(
                    packageName,
                    packagePrefix,
                    method.pathConstant(service),
                    methodSymbol,
                    reservations,
                )
                reserveMethodFamily(packageName, options.runtimePackage, service, method, methodSymbol, reservations)
            }
        }
    }

    val conflict =
        reservations
            .groupBy(DeclarationReservation::key)
            .toSortedMap()
            .values
            .asSequence()
            .map { entries -> entries.distinctBy { it.symbol }.sortedBy { it.symbol } }
            .firstOrNull { entries -> entries.size > 1 }
    if (conflict != null) {
        val first = conflict[0]
        val second = conflict[1]
        throw GeneratorException(
            "generated Kotlin declaration ${first.declaration.quote()} conflicts between protobuf symbols " +
                "${first.symbol.quote()} and ${second.symbol.quote()}",
        )
    }
}

private fun reserveJavaType(
    reservations: MutableList<DeclarationReservation>,
    packageName: String,
    packagePrefix: String,
    descriptor: DescriptorProto,
    symbol: String,
) {
    val name = descriptor.name
    reservations +=
        DeclarationReservation(
            "type:$packageName:$name",
            "class $packagePrefix${kotlinIdent(name)}",
            symbol,
        )
}

private fun reserveJavaType(
    reservations: MutableList<DeclarationReservation>,
    packageName: String,
    packagePrefix: String,
    descriptor: EnumDescriptorProto,
    symbol: String,
) {
    val name = descriptor.name
    reservations +=
        DeclarationReservation(
            "type:$packageName:$name",
            "class $packagePrefix${kotlinIdent(name)}",
            symbol,
        )
}

private fun reserveType(
    packageName: String,
    packagePrefix: String,
    name: String,
    symbol: String,
    reservations: MutableList<DeclarationReservation>,
) {
    reservations +=
        DeclarationReservation(
            "type:$packageName:$name",
            "class $packagePrefix${kotlinIdent(name)}",
            symbol,
        )
}

private fun reserveProperty(
    packageName: String,
    packagePrefix: String,
    name: String,
    symbol: String,
    reservations: MutableList<DeclarationReservation>,
) {
    reservations +=
        DeclarationReservation(
            "property:$packageName:$name",
            "val $packagePrefix${kotlinIdent(name)}",
            symbol,
        )
}

private fun reserveMethodFamily(
    packageName: String,
    runtimePackage: String,
    service: Service,
    method: Method,
    symbol: String,
    reservations: MutableList<DeclarationReservation>,
) {
    val renderedPackage = packageName.split('.').filter(String::isNotEmpty).joinToString(".", transform = ::kotlinIdent)
    val clientOwner = listOf(renderedPackage, kotlinIdent(service.clientName)).filter(String::isNotEmpty).joinToString(".")
    val callOptions = "$runtimePackage.CallOptions"
    val configure = "$callOptions.() -> $callOptions"
    val flowType = "kotlinx.coroutines.flow.Flow"
    val configureErased = "kotlin.jvm.functions.Function1"

    val (parameters, erased) =
        if (method.clientStreaming) {
            listOf("$flowType<${method.input.kotlinType}>", configure) to listOf(flowType, configureErased)
        } else {
            listOf(method.input.kotlinType, configure) to listOf(method.input.kotlinType, configureErased)
        }
    // Unary and client-streaming calls suspend for their response; the others return a flow.
    val isSuspend = !method.serverStreaming
    reserveMember(reservations, clientOwner, method.name, parameters, erased, isSuspend, true, symbol)
}

private fun reserveMember(
    reservations: MutableList<DeclarationReservation>,
    owner: String,
    name: String,
    sourceParameters: List<String>,
    erasedParameters: List<String>,
    isSuspend: Boolean,
    hasDefaultParameters: Boolean,
    symbol: String,
) {
    val renderedName = kotlinIdent(name)
    val sourceSignature = sourceParameters.joinToString(",")
    val erasedJvmParameters = erasedParameters + if (isSuspend) listOf("Continuation") else emptyList()
    reservations +=
        DeclarationReservation(
            "member-0-source:$owner:$name:$sourceSignature",
            "fun $owner.$renderedName(${sourceParameters.joinToString(", ")})",
            symbol,
        )
    reservations +=
        DeclarationReservation(
            "member-1-jvm:$owner:$name:${erasedJvmParameters.joinToString(",")}",
            "JVM method $owner.$renderedName(${erasedJvmParameters.joinToString(", ")})",
            symbol,
        )
    if (hasDefaultParameters) {
        val bridgeParameters = erasedJvmParameters + listOf("Int", "Object")
        reservations +=
            DeclarationReservation(
                "member-2-default-jvm:$owner:${name}\$default:${bridgeParameters.joinToString(",")}",
                "JVM default bridge $owner.$renderedName\$default(${bridgeParameters.joinToString(", ")})",
                symbol = symbol,
            )
    }
}

private fun protoSymbol(
    file: FileDescriptorProto,
    name: String,
): String = listOf(file.`package`, name).filter(String::isNotEmpty).joinToString(".")

private data class Method(
    val name: String,
    val constantName: String,
    val protoName: String,
    val input: TypeRef,
    val output: TypeRef,
    val clientStreaming: Boolean,
    val serverStreaming: Boolean,
    val unreliable: TrevRpcMethodOptions,
    /** The streamed directions that use stream compression. */
    val compressRequests: Boolean,
    val compressResponses: Boolean,
) {
    val kotlinName: String
        get() = kotlinIdent(name)

    /** The wire method path `/package.Service/Method`. */
    fun path(service: Service): String = "/${service.fullProtoName}/$protoName"

    fun pathConstant(service: Service): String = "${service.constantPrefix}_${constantName}_PATH"
}

private data class Service(
    val name: String,
    val clientName: String,
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
        appendLine("@file:JvmName(${kotlinFileFacadeName(file.name).quote()})")
        if (javaPackage(file).isNotEmpty()) {
            appendLine()
            appendLine("package ${javaPackage(file).split('.').joinToString(".", transform = ::kotlinIdent)}")
        }
        appendLine()
        // Only streaming methods take or return a Flow.
        if (services.any { service -> service.methods.any { it.clientStreaming || it.serverStreaming } }) {
            appendLine("import kotlinx.coroutines.flow.Flow")
            appendLine()
        }
        codecs.values.forEach { type -> generateCodec(runtime, type, checkNotNull(codecNames[type.protoName])) }
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
    appendLine(
        "        decode = { body -> $runtime.decodeProtobuf(body, ${type.kotlinType}.getDescriptor(), ${type.kotlinType}::parseFrom) },",
    )
    // The full protobuf name picks the type's compression dictionary.
    appendLine("        messageType = ${type.protoName.removePrefix(".").quote()},")
    appendLine("    )")
    appendLine()
}

private fun describeService(
    file: FileDescriptorProto,
    descriptor: ServiceDescriptorProto,
    types: TypeIndex,
): Service {
    val typeName = javaClassName(descriptor.name)
    val fullProtoName = listOf(file.`package`, descriptor.name).filter(String::isNotEmpty).joinToString(".")
    val methods = descriptor.methodList.map { method -> describeMethod(file, fullProtoName, method, types) }
    return Service(
        name = typeName,
        clientName = "${typeName}Client",
        constantPrefix = screamingSnake(descriptor.name),
        fullProtoName = fullProtoName,
        methods = methods,
    )
}

private fun describeMethod(
    file: FileDescriptorProto,
    serviceName: String,
    descriptor: MethodDescriptorProto,
    types: TypeIndex,
): Method {
    val unreliable = readMethodOptions(descriptor)
    if (unreliable.requests && !descriptor.clientStreaming) {
        throw GeneratorException(
            "$serviceName.${descriptor.name}: unreliable_requests requires a client-streaming or bidirectional method",
        )
    }
    if (unreliable.responses && !descriptor.serverStreaming) {
        throw GeneratorException(
            "$serviceName.${descriptor.name}: unreliable_responses requires a server-streaming or bidirectional method",
        )
    }
    if (unreliable.compressStream && !descriptor.clientStreaming && !descriptor.serverStreaming) {
        throw GeneratorException("$serviceName.${descriptor.name}: compress_stream requires a streaming method")
    }
    val input = types.resolve(file, descriptor.inputType)
    val output = types.resolve(file, descriptor.outputType)
    // Stream compression leaves out directions that are unreliable (their messages are meant for
    // datagrams, which never use the context) and types with redacted fields.
    return Method(
        name = lowerCamel(descriptor.name),
        constantName = screamingSnake(descriptor.name),
        protoName = descriptor.name,
        input = input,
        output = output,
        clientStreaming = descriptor.clientStreaming,
        serverStreaming = descriptor.serverStreaming,
        unreliable = unreliable,
        compressRequests =
            unreliable.compressStream && descriptor.clientStreaming && !unreliable.requests && !types.hasRedactedField(input),
        compressResponses =
            unreliable.compressStream && descriptor.serverStreaming && !unreliable.responses && !types.hasRedactedField(output),
    )
}

/**
 * The `(trevrpc.method)` option of a method: which streamed directions the schema marks unreliable,
 * and whether it allows stream compression.
 */
private data class TrevRpcMethodOptions(
    val requests: Boolean = false,
    val responses: Boolean = false,
    val compressStream: Boolean = false,
) {
    val any: Boolean
        get() = requests || responses
}

/**
 * Reads `(trevrpc.method)` from the unknown fields of `MethodOptions`: the generator's descriptors
 * carry no extension registry, so protoc's extension bytes stay as raw fields, and decoding the
 * bool fields by hand avoids shipping generated option classes. Repeated occurrences merge, as
 * protobuf message fields do.
 */
private fun readMethodOptions(descriptor: MethodDescriptorProto): TrevRpcMethodOptions {
    val field = descriptor.options.unknownFields.getField(METHOD_OPTION_FIELD)
    var result = TrevRpcMethodOptions()
    for (encoded in field.lengthDelimitedList) {
        val fields =
            try {
                UnknownFieldSet.parseFrom(encoded)
            } catch (error: InvalidProtocolBufferException) {
                throw GeneratorException("${descriptor.name}: malformed (trevrpc.method) option: ${error.message}")
            }
        result =
            TrevRpcMethodOptions(
                requests = result.requests || fields.getField(UNRELIABLE_REQUESTS_FIELD).varintList.any { it != 0L },
                responses = result.responses || fields.getField(UNRELIABLE_RESPONSES_FIELD).varintList.any { it != 0L },
                compressStream = result.compressStream || fields.getField(COMPRESS_STREAM_FIELD).varintList.any { it != 0L },
            )
    }
    return result
}

private fun StringBuilder.generateService(
    runtime: String,
    service: Service,
    codecNames: Map<String, String>,
) {
    for (method in service.methods) {
        appendLine(
            "public const val ${method.pathConstant(service)}: String = ${method.path(service).quote()}",
        )
    }
    appendLine()
    generateClient(runtime, service, codecNames)
}

private fun StringBuilder.generateClient(
    runtime: String,
    service: Service,
    codecNames: Map<String, String>,
) {
    appendLine("public class ${kotlinIdent(service.clientName)}(")
    appendLine("    transport: $runtime.RpcTransport,")
    appendLine("    public val defaultCallOptions: $runtime.CallOptions = $runtime.CallOptions(),")
    appendLine(") {")
    appendLine("    private val client: $runtime.Client = $runtime.Client(transport)")
    appendLine()
    for (method in service.methods) {
        generateClientMethod(runtime, service, method, codecNames)
    }
    appendLine("}")
}

private fun StringBuilder.generateClientMethod(
    runtime: String,
    service: Service,
    method: Method,
    codecNames: Map<String, String>,
) {
    val pathConstant = method.pathConstant(service)
    val inputCodec = checkNotNull(codecNames[method.input.protoName])
    val outputCodec = checkNotNull(codecNames[method.output.protoName])
    val input = method.input.kotlinType
    val output = method.output.kotlinType
    val (signature, call) =
        when {
            method.clientStreaming && method.serverStreaming -> {
                "public fun ${method.kotlinName}(\n        requests: Flow<$input>," to
                    "): Flow<$output> =\n        client.bidirectionalStreaming($pathConstant, requests, $inputCodec, $outputCodec, defaultCallOptions.options())"
            }

            method.clientStreaming -> {
                "public suspend fun ${method.kotlinName}(\n        requests: Flow<$input>," to
                    "): $output =\n        client.clientStreaming($pathConstant, requests, $inputCodec, $outputCodec, defaultCallOptions.options())"
            }

            method.serverStreaming -> {
                "public fun ${method.kotlinName}(\n        request: $input," to
                    "): Flow<$output> =\n        client.serverStreaming($pathConstant, request, $inputCodec, $outputCodec, defaultCallOptions.options())"
            }

            else -> {
                "public suspend fun ${method.kotlinName}(\n        request: $input," to
                    "): $output =\n        client.unary($pathConstant, request, $inputCodec, $outputCodec, defaultCallOptions.options())"
            }
        }
    val compressStream =
        when {
            method.compressRequests && method.compressResponses -> "BOTH"
            method.compressRequests -> "REQUESTS"
            method.compressResponses -> "RESPONSES"
            else -> null
        }
    val start = length
    appendLine("    $signature")
    appendLine("        options: $runtime.CallOptions.() -> $runtime.CallOptions = { this },")
    // The method's stream compression goes after the options, as the runtime's last argument.
    appendLine(if (compressStream == null) "    $call" else "    ${call.removeSuffix(")")}, $runtime.CompressStream.$compressStream)")
    if (method.unreliable.any) documentUnreliable(method, start)
    appendLine()
}

/**
 * Inserts a KDoc block before each public function generated since [start]. The schema marks the
 * method's messages unreliable, but Cronet exposes no datagram API, so every message travels
 * reliably on the HTTP stream.
 */
private fun StringBuilder.documentUnreliable(
    method: Method,
    start: Int,
) {
    val directions =
        listOfNotNull(
            "requests".takeIf { method.unreliable.requests },
            "responses".takeIf { method.unreliable.responses },
        ).joinToString(" and ")
    val generated = substring(start)
    setLength(start)
    for (line in generated.lineSequence()) {
        if (line.startsWith("    public suspend fun ") || line.startsWith("    public fun ")) {
            appendLine("    /**")
            appendLine("     * The schema marks this method's $directions unreliable (datagram messages). Cronet has no")
            appendLine("     * datagram API, so this client delivers every message reliably and in order on the stream.")
            appendLine("     */")
        }
        append(line).append('\n')
    }
    setLength(length - 1)
}

private fun javaTypePrefix(file: FileDescriptorProto): List<String> {
    val prefix = javaPackage(file).split('.').filter(String::isNotEmpty)
    return if (file.options.javaMultipleFiles) prefix else prefix + javaOuterClassName(file)
}

private fun javaPackage(file: FileDescriptorProto): String = if (file.options.hasJavaPackage()) file.options.javaPackage else file.`package`

private fun javaOuterClassName(file: FileDescriptorProto): String {
    if (file.options.hasJavaOuterClassname()) return file.options.javaOuterClassname
    val filename = file.name.substringAfterLast('/')
    val basename =
        when {
            filename.endsWith(".protodevel") -> filename.removeSuffix(".protodevel")
            filename.endsWith(".proto") -> filename.removeSuffix(".proto")
            else -> filename
        }
    var candidate = protocCamelCase(basename)
    val topLevelNames =
        buildSet {
            file.messageTypeList.mapTo(this, DescriptorProto::getName)
            file.enumTypeList.mapTo(this, EnumDescriptorProto::getName)
            file.serviceList.mapTo(this, ServiceDescriptorProto::getName)
        }
    if (candidate in topLevelNames) candidate += "OuterClass"
    if (candidate in topLevelNames) {
        throw GeneratorException(
            "protobuf file ${file.name.quote()} requires an explicit java_outer_classname because protoc-derived " +
                "outer class ${candidate.quote()} conflicts with a top-level declaration",
        )
    }
    return candidate
}

private fun protocCamelCase(input: String): String {
    val output = StringBuilder(input.length)
    var capitalizeNext = true
    input.forEachIndexed { index, character ->
        when (character) {
            in 'a'..'z' -> {
                output.append(if (capitalizeNext) character.uppercaseChar() else character)
                capitalizeNext = false
            }

            in 'A'..'Z' -> {
                output.append(if (index == 0 && !capitalizeNext) character.lowercaseChar() else character)
                capitalizeNext = false
            }

            in '0'..'9' -> {
                output.append(character)
                capitalizeNext = true
            }

            else -> {
                capitalizeNext = true
            }
        }
    }
    if (input.endsWith('#')) output.append('_')
    return output.toString()
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

private fun kotlinFileFacadeName(protoFileName: String): String {
    val digest = MessageDigest.getInstance("SHA-256").digest(protoFileName.encodeToByteArray())
    return "TrevRpcGenerated_" +
        buildString(digest.size * 2) {
            for (byte in digest) {
                val value = byte.toInt() and 0xff
                append(HEX_DIGITS[value ushr 4])
                append(HEX_DIGITS[value and 0x0f])
            }
        }
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
