package com.bridge.data

import com.fasterxml.jackson.core.type.TypeReference
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
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
