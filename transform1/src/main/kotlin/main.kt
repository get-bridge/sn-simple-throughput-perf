import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.pulsar.source.PulsarSource
import org.apache.flink.connector.pulsar.sink.PulsarSink
import com.google.gson.Gson
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.api.SubscriptionType
import org.apache.pulsar.client.impl.MessageIdImpl
import java.util.*

data class UserRecord(val id: Int, val name: String, val op: String)

object EnvironmentFactory {
    fun create(
        withWebUI: Boolean = false
    ): StreamExecutionEnvironment {
        val environment = if (withWebUI) {
            val flinkConfig = Configuration()
            StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
        } else {
            StreamExecutionEnvironment.getExecutionEnvironment()
        }

        return environment.apply {
            config.disableGenericTypes()
        }
    }
}

fun main(args: Array<String>) {
    val parameters = ParameterTool.fromArgs(args)

    val env = EnvironmentFactory.create(
        withWebUI = false
    )

    env.config.globalJobParameters = parameters

    val sourceTopic = "debezium.public.users"
    val sinkTopic = "debezium.public.users-transformed"
    val pulsarServiceUrl = "pulsar://localhost:6650"

    val pulsarSource = PulsarSource
        .builder<String>()
        .setServiceUrl(pulsarServiceUrl)
        .setTopics(sourceTopic)
        .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(SimpleStringSchema()))
        .setSubscriptionType(SubscriptionType.Shared)
        .build()

    val pulsarSink = PulsarSink
        .builder<String>()
        .setServiceUrl(pulsarServiceUrl)
        .setTopics(sinkTopic)
        .setSerializationSchema(PulsarSerializationSchema.flinkSchema(SimpleStringSchema()))
        .build()

    val gson = Gson()

    env.fromSource(pulsarSource, WatermarkStrategy.noWatermarks(), "Pulsar Source")
        .map { gson.fromJson(it, UserRecord::class.java) }
        .filter { it.op == "c" || it.op == "u" }
        .map { UserRecord(it.id, it.name.uppercase(Locale.getDefault()), it.op) }
        .map { gson.toJson(it) }
        .sinkTo(pulsarSink)

    // Step 5: Execute the Flink job
    env.execute("Debezium Transformation Job")
}
