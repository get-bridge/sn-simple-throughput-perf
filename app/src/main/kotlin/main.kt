import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.future.asDeferred
import kotlinx.coroutines.runBlocking
import org.apache.pulsar.client.api.CompressionType
import org.apache.pulsar.client.api.ProducerAccessMode
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.Schema
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit

fun main(args: Array<String>) {
    val client = PulsarClient.builder()
        .serviceUrl("pulsar://localhost:6650")
        .build()

    val producer = client.newProducer(Schema.STRING)
        .producerName("messaging-benchmark-producer")
        .accessMode(ProducerAccessMode.Shared)
        .topic("my-topic")
        .enableBatching(false)
        .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
        .batchingMaxMessages(1000)
        .batchingMaxBytes(1024 * 1024)
        .compressionType(CompressionType.NONE)
        .maxPendingMessages(100000)
        .blockIfQueueFull(true)
        .create()

    runBlocking {
        val numberOfBatches = 100
        for (i in (0..numberOfBatches)) {
            val numberOfMessagesInBatch = 100000
            println("BATCH $i : Producing $numberOfMessagesInBatch messages...")
            val start = System.currentTimeMillis()

            (0..numberOfMessagesInBatch)
                .map { producer.sendAsync("MESSAGE $it").asDeferred() }
                .awaitAll()

            val end = System.currentTimeMillis()
            val duration = end - start
            val avgTimePerMessage = duration.toFloat() / numberOfMessagesInBatch
            val avgRate = 1000 / avgTimePerMessage

            println("BATCH $i : Done. Took $duration ms")
            println("BATCH $i : Average rate: $avgRate msg/s")
        }

        producer.close()
        client.close()
    }
}
