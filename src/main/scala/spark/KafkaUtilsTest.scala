package spark

import Mlib.OAuthUtils
import Mlib.TweetSentimentAnalyzer.createSparkStreamingContext
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status
import twitter4j.auth.OAuthAuthorization

object KafkaUtilsTest {
    def main(args: Array[String]): Unit = {

        val ssc: StreamingContext = StreamingContext.getActiveOrCreate(createSparkStreamingContext)
        val oAuth: Some[OAuthAuthorization] = OAuthUtils.bootstrapTwitterOAuth()

        val rawTweets: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, oAuth)

        val preferredHosts = LocationStrategies.PreferConsistent
        val topics = Array("tweets")
        val kafkaParams = Map[String, Object](
            //            "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
            "bootstrap.servers" -> "localhost:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "group1",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )
        val stream = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )
        stream.map(record => (record.key, record.value))
//        dstream.foreachRDD(rdd => {
//            println(rdd.toString)
//        })

        ssc.start
        // the above code is printing out topic details every 5 seconds
        // until you stop it.
        ssc.stop(stopSparkContext = false)
    }

}
