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
    val config = new SparkConf().setAppName("twitter-stream-sentiment")
    val sc = new SparkContext(config)
    sc.setLogLevel("WARN")
    val ssc = new StreamingContext(sc, Seconds(5))
    System.setProperty("twitter4j.oauth.consumerKey", "e4dhcvBgVanr1dEg0H85RPNSn")
    System.setProperty("twitter4j.oauth.consumerSecret", "iax8ymF6PopnMxhxbESayIKeAkfbXbGQBdyAmki3IAGlTUZowJ")
    System.setProperty("twitter4j.oauth.accessToken", "331813667-4p6698Qt0eM4dRjNSF4PxZST19dGz3grq52pPtkY")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "Oa7ooLmnyOUYQ0kTuHvD0oO8NEAvpKdSb1HpM46Ya6ZjV")
    val stream = TwitterUtils.createStream(ssc, None,Array("strike","Strike"))


    //stream.foreachRDD(rdd => println(rdd))
    // Your code here

    val data = stream.map {status => (status.getHashtagEntities.map(_.getText),status.getText(),status.getGeoLocation().getLatitude(),status.getGeoLocation().getLongitude,status.getPlace().getCountry())  }
    val tags = stream.flatMap(status => status.getHashtagEntities.map(_.getText))

    data.saveAsTextFiles("tweets-data"+Calendar.getInstance().getTime())
    data.saveAsTextFiles("tweets-tags"+Calendar.getInstance().getTime())
    ssc.start()
    ssc.awaitTermination()

  }

}
