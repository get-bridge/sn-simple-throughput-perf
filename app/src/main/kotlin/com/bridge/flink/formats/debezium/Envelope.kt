package com.bridge.flink.formats.debezium

interface Envelope<T> {
    val op: Op
    val tsMs: Long
    val source: Source
    val payload: Payload<T>
}
