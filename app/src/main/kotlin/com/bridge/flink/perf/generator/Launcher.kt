package com.bridge.flink.perf.generator

import com.bridge.flink.perf.generator.commands.StreamNativeScenarios
import picocli.CommandLine
import kotlin.system.exitProcess

@CommandLine.Command(name = "generator", subcommands = [
    StreamNativeScenarios::class
])
class Launcher

fun main(args: Array<String>) {
    val commandLine = CommandLine(Launcher())
    exitProcess(commandLine.execute(*args))
}
