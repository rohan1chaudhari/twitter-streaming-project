package dsti

import java.util.Calendar

import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
 * @author ${user.name}
 */
object Streamer {

  // Your code here
  
  def main(args : Array[String]) {
    println("Hello World ! ")
    if(args.length!=1)
      println("Parameter error")
      val x = args(0).toInt

    //Config
    val config = new SparkConf().setAppName("twitter-stream-sentiment") //Setting the app name
    val sc = new SparkContext(config)

    //Setting the logLevel to WARN
    sc.setLogLevel("WARN")

    //BatchDuration in seconds
    val ssc = new StreamingContext(sc, Seconds(10))

    System.setProperty("twitter4j.oauth.consumerKey", "e4dhcvBgVanr1dEg0H85RPNSn")
    System.setProperty("twitter4j.oauth.consumerSecret", "iax8ymF6PopnMxhxbESayIKeAkfbXbGQBdyAmki3IAGlTUZowJ")
    System.setProperty("twitter4j.oauth.accessToken", "331813667-4p6698Qt0eM4dRjNSF4PxZST19dGz3grq52pPtkY")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "Oa7ooLmnyOUYQ0kTuHvD0oO8NEAvpKdSb1HpM46Ya6ZjV")

    //Filtering the tweets with the name of the game whose user reviews we will review
    val stream = TwitterUtils.createStream(ssc, None,Array("Fortnite", "fortnite"))


    // Your code here


    /*
    Collected the tweet text, Date and time of Creation, Favourite count, the count of statuses tweeted by the user,
     and the sum of people that will see that tweet(friends and followers)

    The tweet texts had newline characters that needed to be replaced by a " "
      */

    val data = stream.map {status => ""+status.getText().replaceAll("\n"," ")+","+status.getCreatedAt()+","+status.getFavoriteCount()+","
      +status.getUser().getStatusesCount()+","+status.getUser().getFollowersCount+status.getUser().getFriendsCount()  }

    /*
    Tags used in the tweets
     */

    val tags = stream.flatMap(status => status.getHashtagEntities.map(_.getText))

    /*
    Saving the data in HDFS
    The data is stored in many directories and merging of the files should be done before proceeding to import in Hive
    */

    //   To merge use linux command: cat /students/rchaudhari/tweets-data*/part*

    data.saveAsTextFiles("/students/rchaudhari/tweets-data")
    tags.saveAsTextFiles("/students/rchaudhari/tweets-tags")


    // Starting the Streaming context
    ssc.start()

    //Stopping the Streaming context after number of *minutes* specified by the user (converted into ms)
    ssc.awaitTerminationOrTimeout(x*60*1000)

  }

}
