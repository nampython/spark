package spark

import Mlib.{MLlibSentimentAnalyzer, PropertiesLoader, StopwordsLoader}
import net.liftweb.json._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}
import spark.App.{sc, uri2}

import scala.collection.mutable.ListBuffer


// {"id":1539262858055270400,"timestamp":"Tue Jun 21 15:03:58 +0000 2022","tweet64":"@US_FDA do your job! @RealCandaceO @GovRonDeSantis @BrianKempGA @NewsNation @ABC @FoxNews @CBS a vaccine that works still being ignored, no adverse side effects. @P_McCulloughMD @ComicDaveSmith","location":null}
//{"id":1539262864833228800,"timestamp":"Tue Jun 21 15:03:59 +0000 2022","tweet64":"+ summer is coming. I have many memories of high school in the summer.I remember going to the pool with my friends with the free shuttle bus during the summer holidays, I used the same pool every summer with my friends before COVID-19 and before I became a trainee.  +","location":"she/her 04L"}
//{"id":1539262857371652000,"timestamp":"Tue Jun 21 15:03:57 +0000 2022","tweet64":"@CDCDirector If you inject a child with a vaccine that don't prevent anything you should be charged with child indangerment.   Covid-19 vaccine is increasing dr. Fauci royalty payments nothing more.","location":null}


object SparkStreaming {
    case class Tweet(id: Long, timestamp: Option[String], tweet64: String, screen_name: String, url_img: String, latitude: Double, longitude: Double, lang: String) {
        def this(id: Long, tweet64: String, screen_name: String, url_img: String, latitude: Double, longitude: Double, lang: String) = this(id, None, tweet64, screen_name, url_img, latitude, longitude, lang)
    }
    val uri1 = "mongodb://127.0.0.1/test.myCollection"
    val uri2 = "mongodb+srv://nam130599:nam130599@cluster0.ebeqc.mongodb.net/M001.tweets"

    val kafkaParams: Map[String, Object] = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "test",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topics: Array[String] = Array("tweets")
    val sparkConfig: SparkConf = new SparkConf()
        .setAppName("My App")
        .setMaster("local[*]")
        .registerKryoClasses(Array(classOf[DefaultFormats]))
    implicit val formats: DefaultFormats.type = DefaultFormats
    val spark: SparkSession = SparkSession
        .builder()
        .config(sparkConfig)
        .config("spark.mongodb.input.uri", uri2)
        .config("spark.mongodb.output.uri", uri2)
        .getOrCreate()
    val sc: SparkContext = spark.sparkContext;
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5));
    val stopWordsList: Broadcast[List[String]] = ssc.sparkContext.broadcast(StopwordsLoader.loadStopWords(PropertiesLoader.nltkStopWords))
    //         Load Naive Bayes Model from the location specified in the config file.
    val naiveBayesModel: NaiveBayesModel = NaiveBayesModel.load(ssc.sparkContext, PropertiesLoader.naiveBayesModelPath)

    val stream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
        ssc,
        PreferConsistent,
        Subscribe[String, String](topics, kafkaParams)
    )

    def predictSentiments(textTweet: String, lang: String): Int = {
        if (isTweetInEnglish(lang)) {
            val mllibSentiment = MLlibSentimentAnalyzer.computeSentiment(textTweet, stopWordsList, naiveBayesModel)
            mllibSentiment
        }
        else {
            0;
        }
    }

    def processTweet(tweets: DStream[String]): DStream[(Long, String, String, String, String, Double, Double, Int, String, String)] ={
        val metricsStream = tweets.flatMap(eTweet => {
            implicit val formats: DefaultFormats.type = DefaultFormats
            val relList = ListBuffer[String]()
            // convert a String to a JValue object
            val jValue = parse(eTweet)
            // create a tweet object from the string
            val tweet = jValue.extract[Tweet]

            val id = tweet.id.toLong;
            val timestamp = tweet.timestamp;
            val tweet64 = tweet.tweet64
                .replaceAll("(\\b\\w*RT)|[^a-zA-Z0-9\\s.,!@]", "")
                .replaceAll("(http\\S+)", "")
                .replaceAll("(@\\w+)", "Foo")
                .replaceAll("^(Foo)", "");
            val screen_name = tweet.screen_name;
            val url_img = tweet.url_img;
            val latitude = tweet.latitude.toDouble;
            val longitude = tweet.longitude.toDouble;
            val lang = tweet.lang
            val score = predictSentiments(tweet64, lang);
            var sentimentType = "Neutral";
            if (score == 1) {
                sentimentType = "Positive";
            }
            if (score == -1) {
                sentimentType = "Negative";
            }
            relList += (id + " /TLOC/ " + timestamp + " /TLOC/ " + tweet64 + " /TLOC/ " + screen_name + " /TLOC/ " + url_img + " /TLOC/ " + latitude + " /TLOC/ " + longitude + " /TLOC/ " +  score + " /TLOC/ " + lang + " /TLOC/ " + sentimentType);
            relList.toList
        })
        val processedTweet = metricsStream.map(line => {
            val Array(id, timestamp, tweet64, screen_name, url_img, latitude, longitude, score, lang, sentimentType) = line.split(" /TLOC/ ")
            (id.toLong, timestamp, tweet64, screen_name, url_img, latitude.toDouble, longitude.toDouble, score.toInt, lang, sentimentType)
        })
        processedTweet;
//    Long, String, String, String, String, Double, Double, Int, String
    }
    /**
     * Checks if the tweet Status is in English language.
     * Actually uses profile's language as well as the Twitter ML predicted language to be sure that this tweet is
     * indeed English.
     *
     * @param status twitter4j Status object
     * @return Boolean status of tweet in English or not.
     */
    def isTweetInEnglish(lang: String): Boolean = {
        lang == "en";
    }
    def main(args : Array[String]): Unit = {

        //    case class Tweet(id: Long, timeStamp: Option[String], tweet64: String, screen_name: String, url_img: String, latitude: Double, longitude: Double)    //        (Long, String, String, String, String, Long, Long, Int)
        //    //        {"id":1544886111981342700,
        //    //            "timestamp":"1657164526349",
        //    //            "tweet64":"Papalii comes in as All Blacks shuffle pack for second Ireland Test | Latest Rugby News | https://t.co/atiLzAazci - https://t.co/2Fu9dWlmiX https://t.co/TTxvEuJq19 SignUp HiPeople https://t.co/WAFlj4p8XC",
        //    //            "screen_name":"JustNowOne",
        //    //            "url_img":"http://pbs.twimg.com/profile_images/1508449203613224960/xhJpUFm0_normal.jpg",
        //    //            "latitude":24.07938752,
        //    //            "longitude":56.9318035}
        val tweets: DStream[String] = stream.map(record => (record.value())).cache()
        val processedTweet: DStream[(Long, String, String, String, String, Double, Double, Int, String, String)] = processTweet(tweets).cache()
        val schema = new StructType()
            .add(StructField("id", LongType, nullable = true))
            .add(StructField("timeStamp", StringType, nullable = true))
            .add(StructField("tweet64", StringType, nullable = true))
            .add(StructField("screen_name", StringType, nullable = true))
            .add(StructField("url_img", StringType, nullable = true))
            .add(StructField("latitude", DoubleType, nullable = true))
            .add(StructField("longitude", DoubleType, nullable = true))
            .add(StructField("score", IntegerType, nullable = true))
            .add(StructField("lang", StringType, nullable = true))
            .add(StructField("sentimentType", StringType, nullable = true))

        val counter = sc.longAccumulator("counter")
        processedTweet.foreachRDD(
            (rdd: RDD[(Long, String, String, String, String, Double, Double, Int, String, String)]) => {
                try {
                    val newRDD = rdd.map(r =>
                        Row(r._1, r._2, r._3, r._4, r._5, r._6, r._7, r._8, r._9, r._10)
                    )
                    val dfTweet = spark.createDataFrame(newRDD, schema).cache()
                    dfTweet.show();
                    dfTweet.repartition(10).write.format("mongo")
                        .mode("append").save()
                } catch {
                    case e: Exception => e.printStackTrace()
                }
            }
        )
        ssc.start() // Start the computation
        ssc.awaitTermination() // Wait for the computation to terminate

    }
}
//{"id":1539249582940594200,"timestamp":"Tue Jun 21 14:11:13 +0000 2022","tweet64":"OCUGEN ANNOUNCES PUBLICATION OF POSITIVE RESULTS OF COVID-19 VACCINE TRIAL FOR CHILDREN 2-18 IN THE LANCET INFECTIOUS DISEASES\n\n$OCGN\n\nhttps://t.co/lRxt4hyTfY","location":"Boca Raton, FL"}
//{"id":1539249585570336800,"timestamp":"Tue Jun 21 14:11:13 +0000 2022","tweet64":"FDA \"approves\" COVID Vaccine for 6-month-old Babies despite Deaths among Children increasing by 53% in 2021 following Covid-19 Vaccination https://t.co/9byowyKsJT","location":null}