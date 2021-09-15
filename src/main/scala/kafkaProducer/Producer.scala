package kafkaProducer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.slf4j.LoggerFactory
import java.io.File

class Producer(topic: String, brokers: String) {

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("kafkaProducer")
    .getOrCreate()

  LoggerFactory.getLogger(spark.getClass)
  spark.sparkContext.setLogLevel("WARN")

  val mySchema = StructType(
    StructField("id", IntegerType) ::
      StructField("firstName", StringType) ::
      StructField("lastName", StringType) ::
      StructField("yearOfBirth", IntegerType) ::
      StructField("address", StructType(
        StructField("country", StringType) ::
          StructField("city", StringType) ::
          StructField("street", StringType) :: Nil
      )) ::
      StructField("gender", StringType) :: Nil
  )

  def streamJsonFromFileToKafka(file: String): Unit = {

    val df = spark
      .readStream
      .schema(mySchema)
      .option("multiline", "true")
      .json(file)

    // write json stream to kafka
    import org.apache.spark.sql.functions._
    df.select(to_json(struct("*")).alias("value"))

    df.selectExpr( "to_json(struct(*)) AS value")
      .writeStream
      .format("kafka")
      .outputMode("append")
      .option("checkpointLocation", "E:\\tmp\\kafkaProducer")
      .option("kafka.bootstrap.servers", brokers)
      .option("topic", topic)
      .start()
      .awaitTermination()
  }

}

object Producer {

  def main(args: Array[String]): Unit = {
    val producer = new Producer("test-topic", "localhost:9092")

    val fileLocation =
      new File("./").getCanonicalPath + s"\\src\\main\\resources\\jsonFiles\\*.json"

    producer.streamJsonFromFileToKafka(fileLocation)
  }
}
