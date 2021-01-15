import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object Main {

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

  def filterResults(stream: (String,Int)): String = {
    val streamString = stream.toString
    val sub = streamString.substring(0, streamString.lastIndexOf(","))
    val num = streamString.substring(streamString.lastIndexOf(",") + 1, streamString.length)
    val result = sub + "," + num
    result
  }

  def main(args: Array[String]) {
    setupTwitterStream()
    val key = sys.env.get("AWS_KEY").get
    val sec = sys.env.get("AWS_SECRET").get
    val duration = Minutes(60)
    val ssc = new StreamingContext("local[*]", "TwitterStreaming", duration)
    Logger.getRootLogger().setLevel(Level.ERROR)

    val results = TwitterUtils.createStream(ssc, None)
      .map(status => status.getText)
      .flatMap(text => text.split(" "))
      .filter(word => word.startsWith("#"))
      .map(hashtag => (hashtag, 1))
      .reduceByKeyAndWindow(_+_, Minutes(1440))
      .transform(rdd => rdd.sortBy(x => x._2, ascending = false))
      .map(filterResults)

    //results.foreachRDD(rdd => rdd.coalesce(1).saveAsTextFile(s"s3a://$key:$sec@cpiazza01-revature/project2/Results"))
    results.foreachRDD(rdd => rdd.coalesce(1).saveAsTextFile("Results"))

    ssc.checkpoint("Checkpoint")
    ssc.start()
    ssc.awaitTermination
  }  
}
