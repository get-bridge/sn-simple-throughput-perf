package com.bridge.data

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import kotlin.reflect.KClass
import org.apache.pulsar.functions.api.SerDe

open class JsonSerDe<T: Any>(private val type : KClass<T>): SerDe<T> {
    companion object{
        inline operator fun <reified T: Any> invoke() = JsonSerDe(T::class)
    }

    private var objectMapper = jacksonObjectMapper()

    override fun deserialize(input: ByteArray): T =
        objectMapper.readValue(input, type.javaObjectType)

    override fun serialize(input: T): ByteArray =
        objectMapper.writeValueAsBytes(input)
}
