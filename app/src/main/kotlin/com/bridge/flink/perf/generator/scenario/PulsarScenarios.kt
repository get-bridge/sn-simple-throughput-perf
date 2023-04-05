package com.bridge.flink.perf.generator.scenario

import com.bridge.flink.perf.generator.clients.LearnConsumeRawUserPulsarPerfClient
import com.bridge.flink.perf.generator.clients.LearnGenerateRawUserPulsarPerfClient
import kotlinx.coroutines.coroutineScope
import org.apache.pulsar.client.api.PulsarClient

class PulsarScenarios(private val pulsarClient: PulsarClient) {
    suspend fun generateRawUsers(
        rawTopic: String,
        count: Int,
        parallelism: Int,
        rate: Int,
        acknowledgeWrites: Boolean = true
    ) = coroutineScope {
        val learnGeneratorScenario = LearnGenerateRawUserPulsarPerfClient(
            client = pulsarClient,
            rawTopic = rawTopic
        )

        measureSendRate(
            measurementName = "GenerateUsers",
            count = count,
            parallelism = parallelism,
            rate = rate
        ) {
            learnGeneratorScenario.sendUserAsync().takeIf { acknowledgeWrites }?.await()
        }
    }

    suspend fun consumeRawUsers(
        rawTopic: String,
        expectedRate: Int = 0
    ) = coroutineScope {
        val learnSourceScenario = LearnConsumeRawUserPulsarPerfClient(
            client = pulsarClient,
            rawTopic = rawTopic
        )

        measureReceivingRate(
            measurementName = "ConsumeUsers",
            parallelism = 1,
            expectedRate = expectedRate
        ) { emitStats ->
            learnSourceScenario.consumeUsersAsync().collect { userMessage ->
                emitStats(
                    ReceivingStats(
                        userMessage.tsMs,
                        userMessage.tsMs,
                        0
                    )
                )
            }
        }
    }

}

