package com.bridge.flink.perf.generator.clients

import com.bridge.flink.formats.learn.LearnUserEnvelope
import com.bridge.flink.perf.generator.formats.LearnUserGenerator
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.future.asDeferred
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.ProducerAccessMode
import org.apache.pulsar.client.api.PulsarClient

class LearnGenerateRawUserPulsarPerfClient(
    client: PulsarClient = simpleClient("pulsar://localhost:6650"),
    rawTopic: String
) : PulsarPerfClient(client) {

    private val generator = LearnUserGenerator()

    private val userProducer by lazy {
        newJsonProducer<LearnUserEnvelope>(
            producerName = "simple-generator-learn-raw-user",
            accessMode = ProducerAccessMode.Shared,
            topic = rawTopic
        )
    }

    fun sendUserAsync(): Deferred<MessageId> {
        val envelope = generator.generateUserEnvelope()

        return userProducer
            .newMessage()
            .key(envelope.payload.after.uuid)
            .value(envelope)
            .eventTime(envelope.tsMs)
            .sendAsync()
            .asDeferred()
    }

}

