package com.bridge.flink.formats.learn

import com.bridge.flink.formats.debezium.Envelope
import com.bridge.flink.formats.debezium.Op
import com.bridge.flink.formats.debezium.Payload
import com.bridge.flink.formats.debezium.Source
import com.fasterxml.jackson.annotation.JsonProperty

data class LearnUser(
    val id: Int,
    val email: String,
    val uid: String,
    @JsonProperty("created_at")
    val createdAt: Long,
    @JsonProperty("updated_at")
    val updatedAt: Long,
    @JsonProperty("avatar_url")
    val avatarUrl: String? = null,
    @JsonProperty("first_name")
    val firstName: String? = null,
    @JsonProperty("last_name")
    val lastName: String? = null,
    @JsonProperty("domain_id")
    val domainId: Long,
    @JsonProperty("deleted_at")
    val deletedAt: Long? = null,
    val tagline: String? = null,
    val locale: String,
    @JsonProperty("logged_in_at")
    val loggedInAt: Long,
    val config: String = "{}",
    @JsonProperty("full_name")
    val fullName: String? = null,
    @JsonProperty("sortable_name")
    val sortableName: String? = null,
    val uuid: String,
    @JsonProperty("sub_account_id")
    val subAccountId: Long,
    @JsonProperty("hire_date")
    val hireDate: Long? = null,
    val hidden: Boolean,
    @JsonProperty("job_title")
    val jobTitle: String? = null,
    val bio: String? = null,
    val profile: String? = "{}",
    val department: String? = null,
    val anonymized: Boolean = false
)

data class LearnUserEnvelope(
    override val op: Op,
    @JsonProperty("ts_ms")
    override val tsMs: Long = System.currentTimeMillis(),
    override val source: Source,
    override val payload: LearnPayload
) : Envelope<LearnUser>

data class LearnPayload(
    override val before: LearnUser? = null,
    override val after: LearnUser
) : Payload<LearnUser>
