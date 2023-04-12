package com.bridge.data

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import org.apache.pulsar.functions.api.Context
import org.apache.pulsar.functions.api.Function
import kotlin.reflect.KClass
import org.apache.pulsar.functions.api.SerDe

open class JsonSerDe<T: Any>(private val typeReference: TypeReference<T>): SerDe<T> {

    private var objectMapper = jacksonObjectMapper().apply {
        configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }

    override fun deserialize(input: ByteArray): T =
        objectMapper.readValue(input, typeReference)

    override fun serialize(input: T): ByteArray =
        objectMapper.writeValueAsBytes(input)
}

abstract class DebeziumTransform<X: Any, T: Any>(
    inputType: TypeReference<DebeziumMessage<X>>,
    outputType: TypeReference<T>
): Function<ByteArray, ByteArray> {
    private val inputSerDe = JsonSerDe(inputType)
    private val outputSerDe = JsonSerDe(outputType)

    override fun process(input: ByteArray, context: Context) =
        outputSerDe.serialize(
            transform(inputSerDe.deserialize(input).payload.after)
        )

    abstract fun transform(input: X): T
}
