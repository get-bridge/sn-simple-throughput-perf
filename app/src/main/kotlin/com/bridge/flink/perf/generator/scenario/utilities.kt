package com.bridge.flink.perf.generator.scenario

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.SerializationFeature
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.jsonMapper
import com.fasterxml.jackson.module.kotlin.kotlinModule
import org.apache.pulsar.client.api.schema.SchemaDefinition
import org.apache.pulsar.client.impl.schema.JSONSchema
import org.apache.pulsar.client.impl.schema.reader.JacksonJsonReader
import org.apache.pulsar.client.impl.schema.writer.JacksonJsonWriter

fun customJsonMapper(): ObjectMapper = jsonMapper {
    addModule(kotlinModule())
    addModule(JavaTimeModule())
    disable(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS)
    configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    serializationInclusion(JsonInclude.Include.NON_NULL)
}

inline fun <reified T> customJsonSchema(): JSONSchema<T> {
    val objectMapper = customJsonMapper()

    return JSONSchema.of(
        SchemaDefinition.builder<T>()
            .withPojo(T::class.java)
            .withSchemaReader(JacksonJsonReader(objectMapper, T::class.java))
            .withSchemaWriter(JacksonJsonWriter(objectMapper))
            .build()
    )
}