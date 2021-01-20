package com.revature.mehrab

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Duration, StreamingContext}

object TwitterStreamingRunner {
    def main(args: Array[String]) {
        val env = System.getenv()
        System.setProperty("twitter4j.oath.consumerKey", env.get("CONSUMER_KEY"))
        System.setProperty("twitter4j.oath.consumerSecret", env.get("CONSUMER_SECRET"))
        System.setProperty("twitter4j.oath.accessToken", env.get("ACCESS_TOKEN"))
        System.setProperty("twitter4j.oath.accessTokenSecret", env.get("ACCESS_TOKEN_SECRET"))

        val sparkConf = new SparkConf().setAppName("Twitter DStreams").setMaster("local[4]")
        val context = new SparkContext(sparkConf)
        val streamContext = new StreamingContext(context, Duration(10000))
        val dstream = TwitterUtils.createStream(streamContext, None)

        val hashtags = dstream.flatMap(status => status.getText.split("\\s").filter(_.startsWith("#")))

        val topCounts60secs = hashtags.map((_,1)).reduceByKeyAndWindow(_ + _, Duration(60000))
            .map({
                case (topic, count) => (count, topic) // swap order to sort on count key
            }).transform(_.sortByKey(false))

        topCounts60secs.foreachRDD(rdd => {
            rdd.take(10).foreach(println)
        })
    }
}