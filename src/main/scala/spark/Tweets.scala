package spark

import Mlib.{LogUtils, MLlibSentimentAnalyzer, PropertiesLoader, StopwordsLoader}
import org.apache.spark.mllib.classification.NaiveBayesModel
import org.apache.spark.sql.{Row, SparkSession}

object Tweets {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[1]")
            .appName(this.getClass.getSimpleName)
            .getOrCreate()
        LogUtils.setLogLevels(spark.sparkContext)
        import spark.implicits._
        val stopWordsList = spark.sparkContext.broadcast(StopwordsLoader.loadStopWords(PropertiesLoader.nltkStopWords))
        //        val tweetsDF: DataFrame = loadSentiment140File(spark, PropertiesLoader.sentiment140TrainingFilePath)
        val naiveBayesModel: NaiveBayesModel = NaiveBayesModel.load(spark.sparkContext, PropertiesLoader.naiveBayesModelPath);
        val data = Seq(("boring and predictable", -1),
            ("excellent movie", 1),
            ("extremely mediocre", -1),
            ("A pathetic attempt at a romcom", -1),
            ("Good movie with great actors", 1),
            ("Fantastic job!", 1)).toDF("context", "label")
        data.show()
        val labeledRDD = data.select("context", "label").rdd.map {
            case Row(context: String, label: Int) =>
                val tweetInWords: Seq[String] = MLlibSentimentAnalyzer.getBarebonesTweetText(context, stopWordsList.value)
                (label,
                    (naiveBayesModel.predict(MLlibSentimentAnalyzer.transformFeatures(tweetInWords))) match {
                        case x if x == 0 => -1 // negative
                        case x if x == 2 => 0 // neutral
                        case x if x == 4 => 1 // positive
                        case _ => 0 // if cant figure the sentiment, term it as neutral
                    },
                    context)
        }
        labeledRDD.toDF("Label", "Predict", "Tweet").show()
    }
}
