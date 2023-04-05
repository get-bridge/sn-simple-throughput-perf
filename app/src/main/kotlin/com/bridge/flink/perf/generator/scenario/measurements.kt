package com.bridge.flink.perf.generator.scenario

import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.LongAdder
import kotlin.math.max
import kotlin.math.min

class Metric(val name: String) {
    var minimum: Long? = null
        private set

    var maximum: Long? = null
        private set

    var average: Double = 0.0
        private set

    private var sum: Long = 0

    var count: Long = 0
        private set

    fun reportValue(value: Long) {
        val currentMinimum = minimum
        val currentMaximum = maximum

        sum += value
        count += 1

        minimum = if (currentMinimum == null) value else min(currentMinimum, value)
        maximum = if (currentMaximum == null) value else max(currentMaximum, value)
        average = sum.toDouble() / count
    }

    override fun toString() = "$name (min: $minimum, max: $maximum, avg: ${average.toLong()})"
}

suspend fun measureReceivingRate(
    measurementName: String,
    expectedRate: Int = 0,
    parallelism: Int,
    warmUpSeconds: Int = 10,
    block: suspend ((ReceivingStats) -> Unit) -> Unit
) = coroutineScope {
    val logger = LoggerFactory.getLogger(measurementName)

    logger.info("Spawning $parallelism listeners...")

    val messagesAdder = LongAdder()

    val endToEndProcessingTimes = LongAdder()

    val pureProcessingTimes = LongAdder()

    val sourceTransitTimes = LongAdder()

    val sinkTransitTimes = LongAdder()

    val statistics = launch {
        val messagePerSecondSpan = Metric("messagePerSecondsMinMaxAvg")
        val sourceTransitSpan = Metric("sourceTransitMinMaxAvg")
        val sinkTransitSpan = Metric("sinkTransitMinMaxAvg")
        val endToEndProcessingSpan = Metric("endToEndProcessingMinMaxAvg")

        try {
            var iteration = 0

            while (isActive) {
                delay(1000)

                val messagesPerSecond = messagesAdder.sumThenReset()
                val endToEndProcessingTimeSum = endToEndProcessingTimes.sumThenReset()
                val pureProcessingTimeSum = pureProcessingTimes.sumThenReset()
                val sourceTransitSum = sourceTransitTimes.sumThenReset()
                val sinkTransitSum = sinkTransitTimes.sumThenReset()

                logger.info("Messages per second (Receiving): $messagesPerSecond")

                if (messagesPerSecond != 0L) {
                    val averageSourceTransit = sourceTransitSum / messagesPerSecond
                    val averagePureProcessing = pureProcessingTimeSum / messagesPerSecond
                    val averageSinkTransit = sinkTransitSum / messagesPerSecond
                    val averageEndToEndProcessing = endToEndProcessingTimeSum / messagesPerSecond

                    logger.info("Source transit time (ms) average per second (Receiving): $averageSourceTransit")
                    logger.info("Pure processing time (ms) average per second (Receiving): $averagePureProcessing")
                    logger.info("Sink transit time (ms) average per second (Receiving): $averageSinkTransit")
                    logger.info("End-to-end processing time (ms) average per second (Receiving): $averageEndToEndProcessing")

                    if (iteration == warmUpSeconds) {
                        logger.info("--- Warm-up period has passed. ---")
                    }

                    if (iteration >= warmUpSeconds && (expectedRate == 0 || messagesPerSecond > expectedRate * 0.7)) {
                        sourceTransitSpan.reportValue(averageSourceTransit)
                        sinkTransitSpan.reportValue(averageSinkTransit)
                        endToEndProcessingSpan.reportValue(averageEndToEndProcessing)
                        messagePerSecondSpan.reportValue(messagesPerSecond)
                    }
                }

                iteration += 1
            }
        } finally {
            logger.info("Performance run report ($expectedRate msg/sec) - client parallelism: $parallelism")
            logger.info(messagePerSecondSpan.toString())
            logger.info(sourceTransitSpan.toString())
            logger.info(sinkTransitSpan.toString())
            logger.info(endToEndProcessingSpan.toString())
        }
    }

    val jobs = (1..parallelism).map { instanceId ->
        launch(Dispatchers.IO) {
            logger.info("Listener $instanceId starting up...")

            try {
                block { receivingStats ->
                    val originCreationTime = receivingStats.originCreationTime
                    val ingestTime = receivingStats.ingestTime

                    val endToEndProcessingTime = System.currentTimeMillis() - originCreationTime
                    val sourceTransitTime = ingestTime - originCreationTime
                    val sinkTransitTime = System.currentTimeMillis() - ingestTime

                    val pureProcessingTime = receivingStats.processingDuration

                    messagesAdder.add(1)
                    endToEndProcessingTimes.add(endToEndProcessingTime)
                    pureProcessingTimes.add(pureProcessingTime)
                    sourceTransitTimes.add(sourceTransitTime)
                    sinkTransitTimes.add(sinkTransitTime)
                }
            } catch (e: Exception) {
                if (isActive) {
                    logger.error("Collect failed", e)
                }
            }

            logger.info("Listener $instanceId finished.")
        }
    }

    jobs.forEach { it.join() }

    statistics.cancel()
}

data class ReceivingStats(
    val originCreationTime: Long,
    val ingestTime: Long,
    val processingDuration: Long
)

suspend fun measureSendRate(
    measurementName: String,
    count: Int,
    parallelism: Int,
    rate: Int,
    sendFunction: suspend () -> Unit
) = coroutineScope {
    val logger = LoggerFactory.getLogger(measurementName)

    val infiniteItems = count == -1

    val perInstance = if (infiniteItems) -1 else count / parallelism

    if (infiniteItems) {
        logger.info("Spawning $parallelism instances with infinite items per instance. Use Ctrl+C to terminate.")
    } else {
        logger.info("Spawning $parallelism instances with $perInstance items per instance, $count items overall.")
    }

    val transactions = LongAdder()

    val statistics = launch {
        while (isActive) {
            delay(1000)
            logger.info("Transactions per second (Sending): ${transactions.sumThenReset()}")
        }
    }

    val jobs = (1..parallelism).map { instanceId ->
        launch(Dispatchers.IO) {
            logger.info("Instance $instanceId starting up...")

            var progress = 0
            val reportPer = if (infiniteItems) 10000 else perInstance / 10

            suspend fun send() {
                try {
                    sendFunction()
                    transactions.add(1)

                    progress += 1

                    if (progress % reportPer == 0) {
                        if (infiniteItems) {
                            logger.info("Instance $instanceId sent $progress items so far ...")
                        } else {
                            logger.info("Instance $instanceId sent $progress out of $perInstance ...")
                        }
                    }
                } catch (e: Exception) {
                    if (isActive) {
                        logger.error("Unable to send user", e)
                    }
                }
            }

            while (isActive && (infiniteItems || progress < perInstance)) {
                if (rate == -1) {
                    send()
                } else {
                    val start = System.currentTimeMillis()

                    val actualRate = rate / parallelism

                    repeat(actualRate) {
                        send()
                    }

                    val duration = System.currentTimeMillis() - start

                    if (duration < 1000) {
                        delay(1000 - duration)
                    }
                }
            }

            logger.info("Instance $instanceId finished, $progress items sent.")
        }
    }

    jobs.forEach { it.join() }

    statistics.cancel()
}
