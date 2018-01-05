import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}

object Example {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("SparkAvgJob")
      .getOrCreate()
    val df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "origin")
      .load()

    val schema =
      StructType(
        Array(
          StructField("value", LongType, nullable=false)
        )
      )

    val average =
      df.select(col("key").cast("string"), from_json(df("value").cast("string"), schema) as "json")
        .select(col("key"), col("json")("value") as "value")
        .groupBy("key").avg("value")
        // in order to serialize back to kafka, we need a key and a value
        .select(col("key"), to_json(struct(col("avg(value)") as "value")) as "value");

    val ds = average
      .writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "spark-destination")
      // for recovering
      .option("checkpointLocation", "file:///tmp")
      .outputMode("complete")
      .start()

    ds.awaitTermination()
  }
}
