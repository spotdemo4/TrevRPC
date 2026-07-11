package zip.trev.trevrpc.cronet

import androidx.test.ext.junit.runners.AndroidJUnit4
import androidx.test.platform.app.InstrumentationRegistry
import kotlinx.coroutines.runBlocking
import org.chromium.net.CronetEngine
import org.junit.Assume.assumeTrue
import org.junit.Test
import org.junit.runner.RunWith
import zip.trev.trevrpc.Client
import zip.trev.trevrpc.MessageCodec
import java.util.concurrent.Executors

@RunWith(AndroidJUnit4::class)
class ConfigurableH3InstrumentedTest {
    @Test
    fun unaryAgainstConfiguredEndpoint() {
        val arguments = InstrumentationRegistry.getArguments()
        val endpoint = arguments.getString("trevrpcH3Endpoint")
        val engineSupplierClass = arguments.getString("trevrpcCronetEngineSupplier")
        assumeTrue("trevrpcH3Endpoint is not configured", !endpoint.isNullOrBlank())
        assumeTrue("trevrpcCronetEngineSupplier is not configured", !engineSupplierClass.isNullOrBlank())
        val supplier =
            Class
                .forName(engineSupplierClass)
                .getDeclaredConstructor()
                .newInstance() as CronetEngineSupplier
        val executor = Executors.newSingleThreadExecutor()
        try {
            val transport = CronetRpcTransport(supplier.engine(), endpoint!!, executor)
            runBlocking {
                Client(transport).unary(
                    arguments.getString("trevrpcService") ?: "trevrpc.test.Echo",
                    arguments.getString("trevrpcMethod") ?: "Echo",
                    byteArrayOf(),
                    MessageCodec.BYTE_ARRAY,
                    MessageCodec.BYTE_ARRAY,
                )
            }
        } finally {
            executor.shutdownNow()
        }
    }
}

interface CronetEngineSupplier {
    fun engine(): CronetEngine
}
