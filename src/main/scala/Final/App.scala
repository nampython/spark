package Final

import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import spark.App.{sc, spark}

object App {
    def main(args: Array[String]): Unit = {
        val topics = Array("tweets")
        // Set the Spark StreamingContext to create a DStream for every 15 seconds
        val sparkStreamContext = new StreamingContext(sc, Seconds(5))
        sparkStreamContext.checkpoint("checkpoint")

        val stream = KafkaUtils.createDirectStream[String, String](
            sparkStreamContext,
            PreferConsistent,
            Subscribe[String, String](topics, KafkaConfig.kafkaParams)
        )

        val tweets = stream.map(record => record.value).cache()
        tweets.foreachRDD(rdd => {
            println(rdd)
            sparkStreamContext.start()
            sparkStreamContext.awaitTermination()
        })
    }
}
