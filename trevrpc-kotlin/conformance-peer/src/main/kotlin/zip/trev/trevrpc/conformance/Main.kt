package zip.trev.trevrpc.conformance

import kotlin.system.exitProcess

fun main(arguments: Array<String>) {
    if (!arguments.contentEquals(arrayOf("--protocol", "1"))) {
        System.err.println("usage: trevrpc-conformance-kotlin --protocol 1")
        exitProcess(2)
    }

    val writer = EventWriter(System.out)
    try {
        writer.emit(
            linkedMapOf(
                "schema_version" to PROTOCOL_VERSION,
                "event" to "ready",
                "peer" to PEER_ID,
                "pid" to ProcessHandle.current().pid(),
                "capabilities" to capabilities,
            ),
        )
        val reader = StrictLineReader(System.`in`)
        while (true) {
            when (val parsed = parseCommand(reader.readLine())) {
                ParsedCommand.Stop -> {
                    return
                }

                is ParsedCommand.Run -> {
                    val command = parsed.command
                    val operationResult = dispatch(command)
                    val result =
                        linkedMapOf<String, Any?>(
                            "schema_version" to PROTOCOL_VERSION,
                            "event" to "result",
                            "peer" to PEER_ID,
                            "sequence" to command.sequence,
                            "case_id" to command.caseId,
                            "operation" to command.operation,
                        )
                    result.putAll(operationResult.payload)
                    val error = operationResult.error
                    if (error == null) {
                        result["outcome"] = "success"
                    } else {
                        result["outcome"] = "error"
                        result["category"] = error.category
                        result["status_code"] = error.statusCode
                        System.err.println("${command.caseId} ${error.category}: ${error.native}")
                    }
                    writer.emit(result)
                }
            }
        }
    } catch (error: ProtocolException) {
        runCatching {
            writer.emit(
                linkedMapOf(
                    "schema_version" to PROTOCOL_VERSION,
                    "event" to "fatal",
                    "peer" to PEER_ID,
                    "message" to (error.message ?: "process protocol failure"),
                ),
            )
        }
        System.err.println(error.message)
        exitProcess(2)
    } catch (error: Throwable) {
        System.err.println(error)
        exitProcess(2)
    }
}
