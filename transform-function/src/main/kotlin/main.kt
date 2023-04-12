package com.bridge.data

import com.fasterxml.jackson.core.type.TypeReference

data class InternalUser(val id: Int, val name: String)
data class PublicUser(val id: String, val name: String)

class TransformUser: DebeziumTransform<InternalUser, PublicUser>(
    object : TypeReference<DebeziumMessage<InternalUser>>() {},
    object : TypeReference<PublicUser>() {}
) {
    override fun transform(input: InternalUser) = PublicUser(
        id = "USER#${input.id}",
        name = input.name.uppercase()
    )
}
