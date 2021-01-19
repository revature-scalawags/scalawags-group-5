import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {

  /** Uses system properties to set up twitter credentials
    * taking the information from twitterProps.txt
    * - https://www.udemy.com/course/apache-spark-with-scala-hands-on-with-big-data/learn/lecture/22022796#overview
    */ 
  def setupTwitterStream(){
    import scala.io.Source
    val lines = Source.fromFile("twitterProps")
    for (line <- lines.getLines) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        System.setProperty("twitter4j.oauth." + fields(0), fields(1))
      }
    }
    lines.close()
  }

  def main(args: Array[String]) {

    // Set up Twitter credentials
    setupTwitterStream()

    // Grab key and secret from environment variables
    val key = System.getenv("AWS_ACCESS_KEY_ID")
    val sec = System.getenv("AWS_SECRET_ACCESS_KEY")

    // Will write to results every 10 minutes
    val duration = Seconds(600)

    val ssc = new StreamingContext("local[*]", "TwitterStreaming", duration)
    Logger.getRootLogger().setLevel(Level.ERROR)

    // Filters tweets by hashtags only, 
    // then counts occurances of each one
    // filtering them out every 12 hours
    // and then sorting the results 
    // https://www.udemy.com/course/apache-spark-with-scala-hands-on-with-big-data/learn/lecture/22022796#overview
    val results = TwitterUtils.createStream(ssc, None)
      .map(status => status.getText)
      .flatMap(text => text.split(" "))
      .filter(word => word.startsWith("#"))
      .map(hashtag => (hashtag, 1))
      .reduceByKeyAndWindow(_+_, Minutes(720))
      .transform(rdd => rdd.sortBy(x => x._2, ascending = false)).cache()

    // Save as text file to specified S3 bucket
    results.foreachRDD(rdd => rdd.coalesce(1).saveAsTextFile(s"s3a://${args(0)}"))
    
    results.foreachRDD(rdd => rdd.coalesce(1).saveAsTextFile(s"Results"))

    // Will create streaming checkpoints to ensure accurate data
    ssc.checkpoint("Checkpoint")

    // Start the stream
    ssc.start()

    // Stream until manually terminated
    ssc.awaitTermination
  }  
}
