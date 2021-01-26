package com.revature.mehrab

package com.revature.mehrab

import org.apache.spark.sql.SparkSession

object TwitterKafkaRunner {
    def main(args: Array[String]) {
        val spark = SparkSession.builder()
            .appName("Kafka Consumer")
            .master("local[4]")
            .getOrCreate()
        import spark.implicits._

        val testDF = spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "test").load()

        testDF.select("value").writeStream.outputMode("append").format("console").start().awaitTermination()
    }
}