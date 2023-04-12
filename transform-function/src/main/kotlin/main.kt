package com.bridge.data

import org.apache.pulsar.functions.api.Context
import org.apache.pulsar.functions.api.Function
import org.apache.pulsar.functions.api.SerDe

data class DebeziumMessage<T>(val before: T? = null, val after: T? = null)
data class InternalUser(val id: Int = 0, val name: String = "")
data class PublicUser(val id: String = "", val name: String = "")

class UserMessageSerDe: SerDe<DebeziumMessage<InternalUser>> by JsonSerDe()
class PublicUserSerDe: SerDe<PublicUser> by JsonSerDe()

class TransformUserMessage: Function<DebeziumMessage<InternalUser>, PublicUser> {
    override fun process(input: DebeziumMessage<InternalUser>?, context: Context?) =
        input?.after?.let {
            transformUser(it)
        }
}

fun transformUser(user: InternalUser) = PublicUser(
    id = "USER#${user.id}",
    name = user.name.uppercase()
)
