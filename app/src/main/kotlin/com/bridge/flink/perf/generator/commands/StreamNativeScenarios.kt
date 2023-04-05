package com.bridge.flink.perf.generator.commands

import com.bridge.flink.perf.generator.clients.PulsarPerfClient
import com.bridge.flink.perf.generator.scenario.PulsarScenarios
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import picocli.CommandLine
import java.io.File

@CommandLine.Command(name = "sn")
class StreamNativeScenarios {

    @CommandLine.Option(
        names = ["-p", "--pulsar-broker-url"],
        required = true,
        defaultValue = "pulsar://localhost:6650"
    )
    private lateinit var pulsarUrl: String

    @CommandLine.Option(
        names = ["-pk", "--pulsar-key-file"],
        required = false
    )
    private var keyFile: String? = null

    @CommandLine.Command
    fun justThroughput(
        @CommandLine.Option(names = ["-rt", "--raw-topic"], defaultValue = "persistent://cdc/raw/user") rawTopic: String,
        @CommandLine.Option(names = ["-c", "--count"], defaultValue = "1000", required = true) count: Int,
        @CommandLine.Option(names = ["-cpl", "--consumer-parallelism"], defaultValue = "1", required = true) consumerParallelism: Int,
        @CommandLine.Option(names = ["-ppl", "--producer-parallelism"], defaultValue = "1", required = true) producerParallelism: Int,
        @CommandLine.Option(names = ["-r", "--rate"], defaultValue = "100", required = true) rate: Int,
        @CommandLine.Option(names = ["--fireAndForget"], defaultValue = "false") fireAndForget: Boolean
    ) = runBlocking {
        val logger = LoggerFactory.getLogger("JustThroughput")

        val currentKeyFile = keyFile?.let { "file://${File(it).absolutePath}" }

        val pulsarClient = if (currentKeyFile == null)
            PulsarPerfClient.simpleClient(
                serviceUrl = pulsarUrl,
                numListeners = consumerParallelism
            )
        else
            PulsarPerfClient.streamNativeClient(
                serviceUrl = pulsarUrl,
                credentialsUrl = currentKeyFile,
                numListeners = consumerParallelism
            )

        val consumer = async {
            PulsarScenarios(pulsarClient = pulsarClient).consumeRawUsers(
                rawTopic = rawTopic, expectedRate = rate
            )
        }

        val producer = async {
            PulsarScenarios(pulsarClient = pulsarClient).generateRawUsers(
                rawTopic = rawTopic, count = count, parallelism = producerParallelism, rate = rate, acknowledgeWrites = !fireAndForget
            )
        }

        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                runBlocking {
                    logger.info("Initiating shutdown...")
                    producer.cancelAndJoin()
                    logger.info("Producer cancelled.")
                    consumer.cancelAndJoin()
                    logger.info("Consumer cancelled.")
                }
            }
        })

        producer.await()
        consumer.await()

        CommandLine.ExitCode.OK
    }

}