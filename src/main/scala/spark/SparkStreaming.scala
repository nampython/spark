package spark

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.App.sc

object SparkStreaming {

    val kafkaParams: Map[String, Object] = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "test",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics: Array[String] = Array("test-topic")

    def main(args : Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[1]")
            .appName("My App")
            .getOrCreate()
        val sc: SparkContext = spark.sparkContext;
//        sc.setLogLevel("WARN")

        val ssc = new StreamingContext(sc, Seconds(5));

        val stream  = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )
        stream.map(record=>(record.value().toString)).print
//        stream.map(record => (record.key, record.value))
//        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
//        lines.print()
//        val words: DStream[String] = lines.flatMap(_.split(" "))
//
//        // Count each word in each batch
//        val pairs: DStream[(String, Int)] = words.map(word => (word, 1))
//        val wordCounts: DStream[(String, Int)] = pairs.reduceByKey(_ + _)
//
//        // Print the first ten elements of each RDD generated in this DStream to the console
//        wordCounts.print()


        ssc.start()             // Start the computation
        ssc.awaitTermination()  // Wait for the computation to terminate
    }


}
