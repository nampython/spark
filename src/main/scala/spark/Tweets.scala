package spark


import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import spark.SparkStreaming.{kafkaParams, predictSentiments, sc, spark, ssc, stream, topics}

object Tweets {
    def main(args: Array[String]): Unit = {

        import spark.implicits._
        // Subscribe to 1 topic
        val data = spark
            .readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "tweet")
            .load()
        val df = data.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
            .as[(String, String)].map(
                        x => (x._1, x._2, predictSentiments(x._2, "en"))
                    )
            .toDF("key", "value", "predict")
        val final_data = df.writeStream
            .outputMode("append")
            .format("console")
            .start()

        val uri = "mongodb+srv://nam130599:nam130599@cluster0.ebeqc.mongodb.net/M001.tweet2"
        val uri2 = "mongodb+srv://nam130599:nam130599@cluster0.ebeqc.mongodb.net/M001.tweets"

        val saveToMongDB = df.writeStream
            .format("mongo")
            .option("spark.mongodb.connection.uri", uri2)
            .outputMode("append")
            .start()

        saveToMongDB.awaitTermination()
        final_data.awaitTermination()



//        LogUtils.setLogLevels(spark.sparkContext)
//        import spark.implicits._
//        val stopWordsList = spark.sparkContext.broadcast(StopwordsLoader.loadStopWords(PropertiesLoader.nltkStopWords))
//        //        val tweetsDF: DataFrame = loadSentiment140File(spark, PropertiesLoader.sentiment140TrainingFilePath)
//        val naiveBayesModel: NaiveBayesModel = NaiveBayesModel.load(spark.sparkContext, PropertiesLoader.naiveBayesModelPath);
//        val data = Seq(("boring and predictable", -1),
//            ("excellent movie", 1),
//            ("extremely mediocre", -1),
//            ("A pathetic attempt at a romcom", -1),
//            ("Good movie with great actors", 1),
//            ("Fantastic job!", 1)).toDF("context", "label")
//        data.show()
//        val labeledRDD = data.select("context", "label").rdd.map {
//            case Row(context: String, label: Int) =>
//                val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(context, stopWordsList.value)
//                (label,
//                    (naiveBayesModel.predict(MLlibSentimentAnalyzer.transformFeatures(tweetInWords))) match {
//                        case x if x == 0 => -1 // negative
//                        case x if x == 2 => 0 // neutral
//                        case x if x == 4 => 1 // positive
//                        case _ => 0 // if cant figure the sentiment, term it as neutral
//                    },
//                    context)
//        }
//        labeledRDD.toDF("Label", "Predict", "Tweet").show()
    }
}
