package com.bridge.data

import com.fasterxml.jackson.core.type.TypeReference
import org.apache.pulsar.functions.api.Context
import org.apache.pulsar.functions.api.Function

data class DebeziumMessage<T>(
    val payload: DebeziumPayload<T>
)

data class DebeziumPayload<T>(
    val before: T?,
    val after: T,
    val source: DebeziumSource,
    val op: String,
)

data class DebeziumSource(
    val version: String,
    val connector: String,
    val name: String,
    val snapshot: String,
    val db: String,
    val schema: String,
    val table: String,
    val txId: Int,
    val lsn: Int,
)

data class InternalUser(val id: Int, val name: String)
data class PublicUser(val id: String, val name: String)

class UserMessageSerDe: JsonSerDe<DebeziumMessage<InternalUser>>(
    object : TypeReference<DebeziumMessage<InternalUser>>() {}
)

class PublicUserSerDe: JsonSerDe<PublicUser>(
    object : TypeReference<PublicUser>() {}
)

class TransformUserMessage: Function<DebeziumMessage<InternalUser>, PublicUser> {
    override fun process(input: DebeziumMessage<InternalUser>, context: Context) =
        transformUser(input.payload.after)
}

class TransformStringMessage: Function<ByteArray, ByteArray> {
    private val userMessageSerDe = UserMessageSerDe()
    private val publicUserSerDe = PublicUserSerDe()

    override fun process(input: ByteArray, context: Context) =
        publicUserSerDe.serialize(
            transformUser(userMessageSerDe.deserialize(input).payload.after)
        )
}

fun transformUser(user: InternalUser) = PublicUser(
    id = "USER#${user.id}",
    name = user.name.uppercase()
)
