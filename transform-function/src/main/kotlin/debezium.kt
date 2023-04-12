package com.bridge.data

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
