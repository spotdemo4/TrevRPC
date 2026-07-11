package zip.trev.trevrpc.examples

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import java.nio.file.Path

class CliOptionsTest {
    @Test
    fun `parses harness-compatible single dash options`() {
        val options =
            parseCli(
                arrayOf(
                    "-mode",
                    "lifecycle-client",
                    "-addr",
                    "127.0.0.1:7443",
                    "-cert",
                    "/tmp/server.pem",
                    "-token",
                    "secret",
                    "-iterations",
                    "9",
                    "-transport",
                    "http3",
                ),
                emptyMap(),
            )

        assertEquals(Mode.LIFECYCLE_CLIENT, options.mode)
        assertEquals("127.0.0.1:7443", options.address)
        assertEquals(Path.of("/tmp/server.pem"), options.certificate)
        assertEquals("secret", options.token)
        assertEquals(9, options.iterations)
        assertEquals(ClientTransport.HTTP3, options.transport)
    }

    @Test
    fun `browser mode accepts environment and keeps admission explicit`() {
        val options =
            parseCli(
                arrayOf("-mode", "browser-server"),
                mapOf(
                    "TREVRPC_EXAMPLE_CERT" to "/tmp/browser.pem",
                    "TREVRPC_EXAMPLE_ORIGIN" to "http://127.0.0.1:9000",
                    "TREVRPC_EXAMPLE_AUTHORITIES" to "localhost:7443,127.0.0.1:7443",
                    "TREVRPC_EXAMPLE_MAX_STREAMS" to "1",
                ),
            )

        assertEquals(DEFAULT_BROWSER_TOKEN_FOR_TEST, options.token)
        assertEquals(setOf("http://127.0.0.1:9000"), options.browserOrigins)
        assertEquals(setOf("localhost:7443", "127.0.0.1:7443"), options.browserAuthorities)
        assertEquals(1, options.maxConcurrentStreams)
    }

    @Test
    fun `rejects invalid iterations and unknown options`() {
        assertThrows(IllegalArgumentException::class.java) {
            parseCli(arrayOf("-mode", "client", "-cert", "cert.pem", "-iterations", "0"), emptyMap())
        }
        assertThrows(IllegalArgumentException::class.java) {
            parseCli(arrayOf("-mode", "client", "-cert", "cert.pem", "-unknown", "x"), emptyMap())
        }
    }

    private companion object {
        const val DEFAULT_BROWSER_TOKEN_FOR_TEST = "trevrpc-example-token"
    }
}
