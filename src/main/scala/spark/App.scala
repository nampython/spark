package spark


import com.mongodb.MongoClient
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.StanfordCoreNLP
import edu.stanford.nlp.sentiment._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.{Seconds, StreamingContext}

import java.io.IOException
import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

/**
 * @author ${user.name}
 */
object App {

    val uri1 = "mongodb://127.0.0.1/test.myCollection"
    val uri2 = "mongodb+srv://nam130599:nam130599@cluster0.ebeqc.mongodb.net/M001.test"

    val spark:SparkSession = SparkSession
        .builder()
        .master("local[1]")
        .appName("Twitter Processor")
        .config("spark.mongodb.input.uri", uri2)
        .config("spark.mongodb.output.uri", uri2)
        .getOrCreate()
    spark.conf.set("spark.executor.memory", "5g")

    val sc: SparkContext = spark.sparkContext

    import spark.implicits._
    import spark.sql




    def main(args : Array[String]): Unit = {

        val input: Array[String] = Array("tweets")
        val Array(topics) = input
        val topicSet = topics.split(",").toSet

        println(topicSet)


        sc.setLogLevel("WARN")

        // Set the Spark StreamingContext to create a DStream for every 15 seconds
        val sparkStreamContext = new StreamingContext(sc, Seconds(5))
        sparkStreamContext.checkpoint("checkpoint")

        val kafkaParams = Map[String, Object](
//            "bootstrap.servers" -> "localhost:9092,anotherhost:9092",
            "bootstrap.servers" -> "localhost:9092",
            "key.deserializer" -> classOf[StringDeserializer],
            "value.deserializer" -> classOf[StringDeserializer],
            "group.id" -> "use_a_separate_group_id_for_each_stream",
            "auto.offset.reset" -> "latest",
            "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topics_ = Array("tweets")

        val stream = KafkaUtils.createDirectStream[String, String](
            sparkStreamContext,
            PreferConsistent,
            Subscribe[String, String](topics_, kafkaParams) //
        )


        val tweets = stream.map(record => record.value).cache()


        //val hashTagCountRDD = getHashTagCounts(tweets).cache()
        //val htInfoRDD = hashTagCountRDD.join(hashTagSentimentRDD)
        val hashTagSentimentRDD = processTweet(tweets).cache()


        // Create a data frame and write to database
        val schema = new StructType()
            .add(StructField("timestamp", IntegerType, nullable = true))
            .add(StructField("hashtag", StringType, nullable = true))
            .add(StructField("sentiment-score", DoubleType, nullable = true))
            .add(StructField("sentiment-type", StringType, nullable = true))
            .add(StructField("country", StringType, nullable = true))

        val counter = sc.longAccumulator("counter")

        hashTagSentimentRDD.foreachRDD(
            (rdd: RDD[(String, Double, String, String)], time: org.apache.spark.streaming.Time) => {
            println("Calling hashTagSentimentRDD ...")
            try {
                val newRDD = rdd.map(r =>
                    Row(( time.milliseconds / 1000).toInt, r._1, r._2, r._3, r._4))
                val df = spark.createDataFrame(newRDD, schema).cache()
                if (counter.isZero) {
                    updateDB()
                }
                counter.add(1)
                val process_df = df.dropDuplicates(Seq("timestamp", "hashtag", "country"))
                process_df.show()
                process_df.repartition(10).write.format("mongo")
                    .mode("append").save()

            } catch {
                case e: Exception => e.printStackTrace()
            }
        })
        tweets.count().map(cnt => "Received " + cnt + " kafka messages.").print()
        sparkStreamContext.start()
        sparkStreamContext.awaitTermination()
    }

    def processTweet(tweets: DStream[String]): DStream[(String, Double, String, String)] = {
        println("Calling processTweet ...")
        val metricsStream = tweets.flatMap { eTweet => {
            println(eTweet)
            val retList = ListBuffer[String]()
            // Process each tweet
            for (tag <- eTweet.split(" ")) {
                if (tag.startsWith("#") && tag.replaceAll("\\s", "").length > 1) {
                    val tweetObj = eTweet.split(" /TLOC/ ")
                    // Function call to extract country associated with the tweet
                    val country = GetCountry.getCountryTwitter(tweetObj(0))
                    // Clean hashtags, emoji's, hyperlinks, and twitter tags
                    // which can confuse the model. Replace @mention with generic word Foo
                    val tweet_clean = tweetObj(1)
                        .replaceAll("(\\b\\w*RT)|[^a-zA-Z0-9\\s.,!@]", "")
                        .replaceAll("(http\\S+)", "")
                        .replaceAll("(@\\w+)", "Foo")
                        .replaceAll("^(Foo)", "")
                    try {
                        // Function call to detect the tweet sentiment
                        val (sentimentScore, sentimentType) = DetectSentiment._detectSentiment(tweet_clean)
                        retList += (tag + " /TLOC/ " + sentimentScore + " /TLOC/ " +
                            sentimentType.toString.toLowerCase + " /TLOC/ " + country)

                    } catch {
                        case e: IOException => e.printStackTrace(); (tag, "-1.0")
                    }
                }
            }
            retList.toList
        }}
        val processedTweet = metricsStream.map(line => {
            val Array(tag, sentiScore, sentiType, location) = line.split(" /TLOC/ ")
            (tag.replaceAll("(\\w*RT)|[^a-zA-Z0-9#]", ""),
                sentiScore.toDouble, sentiType, location)
        })
        /*
        // averaging the sentiment for each hash tag (not being used at the moment)
        val htSenti = processedTweet.mapValues(value => (value, 1)).reduceByKey {
          case ((sumL, countL), (sumR, countR)) =>
            (sumL + sumR, countL + countR)
        }.mapValues {
          case (sum , count) => sum / count
        }
        */
        processedTweet
    }

    def updateDB() : Unit = {
        val mongo = new MongoClient("127.0.0.1", 27017)
        val database = mongo.getDatabase("twitter")
        database.drop()
        println("Database deleted successfully")
        mongo.close()
    }
}
