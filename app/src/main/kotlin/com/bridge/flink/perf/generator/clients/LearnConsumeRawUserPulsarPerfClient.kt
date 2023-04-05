package com.bridge.flink.perf.generator.clients

import com.bridge.flink.formats.learn.LearnUserEnvelope
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.channels.awaitClose
import kotlinx.coroutines.channels.trySendBlocking
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.callbackFlow
import kotlinx.coroutines.flow.flowOn
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.PulsarClient
import org.apache.pulsar.client.api.SubscriptionType

class LearnConsumeRawUserPulsarPerfClient(
    client: PulsarClient = simpleClient("pulsar://localhost:6650"),
    private val rawTopic: String
) : PulsarPerfClient(client) {

    suspend fun consumeUsersAsync(): Flow<LearnUserEnvelope> {
        return callbackFlow {
            val consumer = newJsonConsumer<LearnUserEnvelope>(
                subscriptionName = "simple-consume-learn-raw-user",
                subscriptionType = SubscriptionType.Shared,
                topic = rawTopic,
                messageListener = { consumer, message ->
                    try {
                        trySendBlocking(message.value).getOrThrow()
                        consumer.acknowledge(message)
                    } catch (e: Exception) {
                        consumer.negativeAcknowledge(message)
                        cancel(CancellationException("Send error", e))
                    }
                }
            )

            awaitClose {
                consumer.close()
            }
        }.flowOn(Dispatchers.IO)
    }
}
