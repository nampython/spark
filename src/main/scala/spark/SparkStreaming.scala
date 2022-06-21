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

<<<<<<< HEAD

// {"id":1539262858055270400,"timestamp":"Tue Jun 21 15:03:58 +0000 2022","tweet64":"@US_FDA do your job! @RealCandaceO @GovRonDeSantis @BrianKempGA @NewsNation @ABC @FoxNews @CBS a vaccine that works still being ignored, no adverse side effects. @P_McCulloughMD @ComicDaveSmith","location":null}
//{"id":1539262864833228800,"timestamp":"Tue Jun 21 15:03:59 +0000 2022","tweet64":"+ summer is coming. I have many memories of high school in the summer.I remember going to the pool with my friends with the free shuttle bus during the summer holidays, I used the same pool every summer with my friends before COVID-19 and before I became a trainee.  +","location":"she/her 04L"}
//{"id":1539262857371652000,"timestamp":"Tue Jun 21 15:03:57 +0000 2022","tweet64":"@CDCDirector If you inject a child with a vaccine that don't prevent anything you should be charged with child indangerment.   Covid-19 vaccine is increasing dr. Fauci royalty payments nothing more.","location":null}


=======
>>>>>>> origin/master
object SparkStreaming {

    val kafkaParams: Map[String, Object] = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "test",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
    )
<<<<<<< HEAD
    val topics: Array[String] = Array("tweets")
=======
    val topics: Array[String] = Array("test-topic")
>>>>>>> origin/master

    def main(args : Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
<<<<<<< HEAD
            .master("local[*]")
            .appName("My App")
            .getOrCreate()
        val sc: SparkContext = spark.sparkContext;
        sc.setLogLevel("WARN")
=======
            .master("local[1]")
            .appName("My App")
            .getOrCreate()
        val sc: SparkContext = spark.sparkContext;
//        sc.setLogLevel("WARN")
>>>>>>> origin/master

        val ssc = new StreamingContext(sc, Seconds(5));

        val stream  = KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topics, kafkaParams)
        )
<<<<<<< HEAD
        val tweets = stream.map(record=>(record.value().toString)).cache()
        tweets.print()
//        val pairs = words.map(word => (word, 1))
//        val wordCounts = pairs.reduceByKey(_ + _)
//        wordCounts.print()
//                stream.map(record => (record.key, record.value))
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
=======
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
>>>>>>> origin/master


        ssc.start()             // Start the computation
        ssc.awaitTermination()  // Wait for the computation to terminate
    }


}
<<<<<<< HEAD
//{"id":1539249582940594200,"timestamp":"Tue Jun 21 14:11:13 +0000 2022","tweet64":"OCUGEN ANNOUNCES PUBLICATION OF POSITIVE RESULTS OF COVID-19 VACCINE TRIAL FOR CHILDREN 2-18 IN THE LANCET INFECTIOUS DISEASES\n\n$OCGN\n\nhttps://t.co/lRxt4hyTfY","location":"Boca Raton, FL"}
//{"id":1539249585570336800,"timestamp":"Tue Jun 21 14:11:13 +0000 2022","tweet64":"FDA \"approves\" COVID Vaccine for 6-month-old Babies despite Deaths among Children increasing by 53% in 2021 following Covid-19 Vaccination https://t.co/9byowyKsJT","location":null}
=======
>>>>>>> origin/master
