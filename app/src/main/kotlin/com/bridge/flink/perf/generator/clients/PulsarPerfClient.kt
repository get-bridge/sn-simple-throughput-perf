package com.bridge.flink.perf.generator.clients

import com.bridge.flink.perf.generator.scenario.customJsonSchema
import org.apache.pulsar.client.api.*
import org.apache.pulsar.client.impl.auth.oauth2.AuthenticationFactoryOAuth2
import java.net.URL
import java.util.concurrent.CopyOnWriteArrayList

abstract class PulsarPerfClient(
    protected val client: PulsarClient = simpleClient("pulsar://localhost:6650")
) : AutoCloseable {

    protected val producers: MutableList<Producer<*>> = CopyOnWriteArrayList()

    protected val consumers: MutableList<Consumer<*>> = CopyOnWriteArrayList()

    protected inline fun <reified T> newJsonProducer(
        producerName: String,
        accessMode: ProducerAccessMode,
        topic: String
    ): Producer<T> {
        return client.newProducer(customJsonSchema<T>())
            .producerName(producerName)
            .accessMode(accessMode)
            .topic(topic)
            .maxPendingMessages(50000)
            .create().also { producers += it }
    }

    protected inline fun <reified T> newJsonConsumer(
        consumerName: String,
        subscriptionName: String,
        topic: String
    ): Consumer<T> {
        return client.newConsumer(customJsonSchema<T>())
            .subscriptionName(subscriptionName)
            .subscriptionType(SubscriptionType.Key_Shared)
            .consumerName(consumerName)
            .receiverQueueSize(50000)
            .topic(topic)
            .subscribe().also { consumers += it }
    }

    protected inline fun <reified T> newJsonConsumer(
        subscriptionName: String,
        subscriptionType: SubscriptionType = SubscriptionType.Exclusive,
        topic: String,
        messageListener: MessageListener<T>
    ): Consumer<T> {
        return client.newConsumer(customJsonSchema<T>())
            .subscriptionType(subscriptionType)
            .subscriptionName(subscriptionName)
            .receiverQueueSize(50000)
            .topic(topic)
            .messageListener(messageListener)
            .autoScaledReceiverQueueSizeEnabled(true)
            .subscribe().also { consumers += it }
    }

    override fun close() {
        producers.forEach { it.close() }
        consumers.forEach { it.close() }
        client.close()
    }

    companion object {
        fun simpleClient(
            serviceUrl: String,
            numListeners: Int = 1
        ) = PulsarClient.builder()
            .serviceUrl(serviceUrl)
            .listenerThreads(numListeners)
            .build()

        fun streamNativeClient(
            serviceUrl: String = "pulsar+ssl://prod-iad.bridge.snio.cloud:6651",
            credentialsUrl: String,
            issuerUrl: String = "https://auth.streamnative.cloud/",
            audience: String = "urn:sn:pulsar:bridge:north-america",
            numListeners: Int = 1
        ) = PulsarClient.builder()
            .serviceUrl(serviceUrl)
            .listenerThreads(numListeners)
            .authentication(
                AuthenticationFactoryOAuth2.clientCredentials(
                    URL(issuerUrl),
                    URL(credentialsUrl),
                    audience
                )
            )
            .build()
    }

}
