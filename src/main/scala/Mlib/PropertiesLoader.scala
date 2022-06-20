package Mlib

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Exposes all the key-value pairs as properties object using Config object of Typesafe Config project.
 */
object PropertiesLoader {
    private val conf: Config = ConfigFactory.load("application.conf")
    val sentiment140TrainingFilePath: String = conf.getString("SENTIMENT140_TRAIN_DATA_ABSOLUTE_PATH");
    val sentiment140TestingFilePath: String = conf.getString("SENTIMENT140_TEST_DATA_ABSOLUTE_PATH")
    val naiveBayesModelPath: String = conf.getString("NAIVEBAYES_MODEL_ABSOLUTE_PATH")
    val nltkStopWords: String = conf.getString("NLTK_STOPWORDS_FILE_NAME ")
    val modelAccuracyPath: String = conf.getString("NAIVEBAYES_MODEL_ACCURACY_ABSOLUTE_PATH ")

    val microBatchTimeInSeconds: Int = conf.getInt("STREAMING_MICRO_BATCH_TIME_IN_SECONDS")
    val totalRunTimeInMinutes: Int = conf.getInt("TOTAL_RUN_TIME_IN_MINUTES")

    val consumerKey: String = conf.getString("CONSUMER_KEY")
    val consumerSecret: String = conf.getString("CONSUMER_SECRET")
    val accessToken: String = conf.getString("ACCESS_TOKEN_KEY")
    val accessTokenSecret: String = conf.getString("ACCESS_TOKEN_SECRET")

    val tweetsRawPath: String = conf.getString("TWEETS_RAW_ABSOLUTE_PATH")
    val saveRawTweets: Boolean = conf.getBoolean("SAVE_RAW_TWEETS")
    val tweetsClassifiedPath: String = conf.getString("TWEETS_CLASSIFIED_ABSOLUTE_PATH")
}
