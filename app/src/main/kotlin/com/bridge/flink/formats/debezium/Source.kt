package com.bridge.flink.formats.debezium

import com.fasterxml.jackson.annotation.JsonProperty

data class Source(
    val version: String,
    val connector: String,
    val name: String,
    @JsonProperty("ts_ms")
    val tsMs: Long = System.currentTimeMillis(),
    val snapshot: String,
    val db: String,
    val sequence: String,
    val schema: String,
    val table: String,
    val txId: Long,
    val lsn: Long,
    val xmin: Long?
)