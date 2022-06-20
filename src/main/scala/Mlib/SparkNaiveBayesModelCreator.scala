package Mlib

import org.apache.hadoop.io.compress.GzipCodec
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.mllib.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import spark.App.{spark, uri2}


object SparkNaiveBayesModelCreator {

    def main(args: Array[String]): Unit = {
        val spark = createSparkSession();
        LogUtils.setLogLevels(spark.sparkContext)
        val stopWordsList = spark.sparkContext.broadcast(StopwordsLoader.loadStopWords(PropertiesLoader.nltkStopWords))
        createAndSaveNBModel(spark, stopWordsList)
        validateAccuracyOfNBModel(spark, stopWordsList)
    }

    def createSparkSession(): SparkSession = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[1]")
            .appName(this.getClass.getSimpleName)
            .getOrCreate()
//        spark.conf.set("spark.executor.memory", "5g")
        //        val sc = spark.sparkContext;
        spark
    }


    def loadSentiment140File(spark: SparkSession, sentiment140FilePath: String): DataFrame = {
        val sqlContext = SQLContextSingleton.getInstance(spark)

        val tweetsDF = sqlContext.read
            .format("com.databricks.spark.csv")
            .option("header", "false")
            .option("inferSchema", "true")
            .load(sentiment140FilePath)
            .toDF("polarity", "id", "date", "query", "user", "status")

        // Drop the columns we are not interested in.
        tweetsDF.drop("id").drop("date").drop("query").drop("user")
    }

    /**
     * Creates a Naive Bayes Model of Tweet and its Sentiment from the Sentiment140 file.
     *
     * @param sc            -- Spark Context.
     * @param stopWordsList -- Broadcast variable for list of stop words to be removed from the tweets.
     */
    def createAndSaveNBModel(spark: SparkSession, stopWordsList: Broadcast[List[String]]): Unit = {

        val tweetsDF: DataFrame = loadSentiment140File(spark, PropertiesLoader.sentiment140TrainingFilePath)
        tweetsDF.show(10)

        val labeledRDD = tweetsDF.select("polarity", "status").rdd.map {
            case Row(polarity: Int, tweet: String) =>
                val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(tweet, stopWordsList.value)
                LabeledPoint(polarity, MLlibSentimentAnalyzer.transformFeatures(tweetInWords))
        }
        labeledRDD.cache()

        val naiveBayesModel: NaiveBayesModel = NaiveBayes.train(labeledRDD, lambda = 1.0, modelType = "multinomial")
        naiveBayesModel.save(spark.sparkContext, PropertiesLoader.naiveBayesModelPath)
    }


    /**
     * Remove new line characters.
     *
     * @param tweetText -- Complete text of a tweet.
     * @return String with new lines removed.
     */
    def replaceNewLines(tweetText: String): String = {
        tweetText.replaceAll("\n", "")
    }

    /**
     * Validates and check the accuracy of the model by comparing the polarity of a tweet from the dataset and compares it with the MLlib predicted polarity.
     *
     * @param sc            -- Spark Context.
     * @param stopWordsList -- Broadcast variable for list of stop words to be removed from the tweets.
     */
    def validateAccuracyOfNBModel(spark: SparkSession, stopWordsList: Broadcast[List[String]]): Unit = {
        val naiveBayesModel: NaiveBayesModel = NaiveBayesModel.load(spark.sparkContext, PropertiesLoader.naiveBayesModelPath)
        val tweetsDF: DataFrame = loadSentiment140File(spark, PropertiesLoader.sentiment140TestingFilePath)
        val actualVsPredictionRDD = tweetsDF.select("polarity", "status").rdd.map {
            case Row(polarity: Int, tweet: String) =>
                val tweetText = replaceNewLines(tweet)
                val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(tweetText, stopWordsList.value)
                (polarity.toDouble,
                    naiveBayesModel.predict(MLlibSentimentAnalyzer.transformFeatures(tweetInWords)),
                    tweetText)
        }
        val accuracy = 100.0 * actualVsPredictionRDD.filter(x => x._1 == x._2).count() / tweetsDF.count()
        println(f"""\n\t<==******** Prediction accuracy compared to actual: $accuracy%.2f%% ********==>\n""")
        saveAccuracy(spark, actualVsPredictionRDD)
    }
    /**
     * Saves the accuracy computation of the ML library.
     * The columns are actual polarity as per the dataset, computed polarity with MLlib and the tweet text.
     *
     * @param sc                    -- Spark Context.
     * @param actualVsPredictionRDD -- RDD of polarity of a tweet in dataset and MLlib computed polarity.
     */
    def saveAccuracy(spark: SparkSession, actualVsPredictionRDD: RDD[(Double, Double, String)]): Unit = {
        val sqlContext = SQLContextSingleton.getInstance(spark)
        import sqlContext.implicits._
        val actualVsPredictionDF = actualVsPredictionRDD.toDF("Actual", "Predicted", "Text")
        actualVsPredictionDF.coalesce(1).write
            .format("com.databricks.spark.csv")
            .option("header", "true")
            .option("delimiter", "\t")
            // Compression codec to compress while saving to file.
            .option("codec", classOf[GzipCodec].getCanonicalName)
            .mode(SaveMode.Append)
            .save(PropertiesLoader.modelAccuracyPath)
    }
}
