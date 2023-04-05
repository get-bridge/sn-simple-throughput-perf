package com.bridge.flink.formats.debezium

import com.fasterxml.jackson.annotation.JsonValue

enum class Op(@get:JsonValue val code: String) {
    READ("r"),
    CREATE("c"),
    UPDATE("u"),
    DELETE("d"),
    TRUNCATE("t"),
    MESSAGE("m"),
    UNKNOWN("_");
}