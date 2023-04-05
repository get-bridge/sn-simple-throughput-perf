package com.bridge.flink.formats.debezium

interface Payload<T> {
    val before: T?
    val after: T
}