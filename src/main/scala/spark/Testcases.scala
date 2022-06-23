package spark

import net.liftweb.json._
import org.apache.commons.net.ntp.TimeStamp
import scala.util.parsing.json._
import java.util.Date
//{"id":1539249582940594200,"timestamp":"Tue Jun 21 14:11:13 +0000 2022","tweet64":"OCUGEN ANNOUNCES PUBLICATION OF POSITIVE RESULTS OF COVID-19 VACCINE TRIAL FOR CHILDREN 2-18 IN THE LANCET INFECTIOUS DISEASES\n\n$OCGN\n\nhttps://t.co/lRxt4hyTfY","location":"Boca Raton, FL"}


case class Tweet(id: String, timeStamp: String, tweet64: String, location: String) {
//    def this(id: String, tweet64: String, location: String) = this(id, None, tweet64, location)
}

object Testcases {
    def main(args : Array[String]): Unit = {

        implicit val formats: DefaultFormats.type = DefaultFormats

        val tweetString: String = """{"id":1539249582940594200,"timestamp":"Tue Jun 21 14:11:13 +0000 2022","tweet64":"OCUGEN ANNOUNCES PUBLICATION OF POSITIVE RESULTS OF COVID-19 VACCINE TRIAL FOR CHILDREN 2-18 IN THE LANCET INFECTIOUS DISEASES\n\n$OCGN\n\nhttps://t.co/lRxt4hyTfY","location":"Boca Raton, FL"}"""

        // convert a String to a JValue object
        val jValue = parse(tweetString)
        println(jValue)

        // create a tweet object from the string
        val tweet = jValue.extract[Tweet]

        println("id: " + tweet.id)
        println("timeStamp: " +  tweet.timeStamp)
        println("tweet64: " + tweet.tweet64
            .toLowerCase()
            .replaceAll("(\\b\\w*RT)|[^a-zA-Z0-9\\s.,!@]", "")
            .replaceAll("(http\\S+)", "")
            .replaceAll("(@\\w+)", "")
            .replaceAll("^(Foo)", "")
            .replaceAll("\n", "")
            .replaceAll("rt\\s+", "")
            .replaceAll("\\s+@\\w+", "")
            .replaceAll("@\\w+", "")
            .replaceAll("\\s+#\\w+", "")
            .replaceAll("#\\w+", "")
            .replaceAll("(?:https?|http?)://[\\w/%.-]+", "")
            .replaceAll("(?:https?|http?)://[\\w/%.-]+\\s+", "")
            .replaceAll("(?:https?|http?)//[\\w/%.-]+\\s+", "")
            .replaceAll("(?:https?|http?)//[\\w/%.-]+", "")
        )
        println("location: " + tweet.location)
    }
}
