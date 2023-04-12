import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.from_json
import org.apache.spark.sql.functions.to_json
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.StructType
import org.jetbrains.kotlinx.spark.api.*

data class User(val id: Int, val name: String)
data class DebeziumMessage(val before: User?, val after: User)

fun transformUser(user: User): User {
    return User(user.id, user.name.uppercase())
}

const val pulsarServiceUrl = "pulsar://localhost:6650"
const val adminUrl = "http://localhost:8080"
const val topicSource = "persistent://public/default/users"
const val topicDestination = "persistent://public/default/users-transformed"

fun main(args: Array<String>) = withSpark(appName = "Pulsar Transform Users") {
    val transformDebeziumRecordUDF by udf { record: DebeziumMessage -> transformUser(record.after) }
    transformDebeziumRecordUDF.register()

    val df = spark.readStream()
        .format("pulsar")
        .option("service.url", pulsarServiceUrl)
        .option("admin.url", adminUrl)
        .option("topic", topicSource)
        .load()

    val parsedDF = df
        .select(from_json(col("value").cast("string"), debeziumSchema).getField("after").alias("user"))
        .select(col("user"))

    val transformedDF = parsedDF.select(transformDebeziumRecordUDF(col("message")).alias("user"))
        .select(to_json(col("user")).alias("value"))

    val query = transformedDF.writeStream()
        .format("pulsar")
        .option("service.url", pulsarServiceUrl)
        .option("admin.url", adminUrl)
        .option("topic", topicDestination)
        .start()

    query.awaitTermination()
}
