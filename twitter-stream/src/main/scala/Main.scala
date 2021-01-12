import org.apache.spark.streaming._
import org.apache.spark.streaming.twitter._
import java.util.Calendar
import java.io.PrintWriter
import org.apache.log4j.{Level, Logger}

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

  def main(args: Array[String]) {

    Logger.getRootLogger.setLevel(Level.ERROR)
    setupTwitterStream()

    val duration = Seconds(10)


    val ssc = new StreamingContext("local[*]", "TwitterStreaming", duration)

    val rawTweets = TwitterUtils.createStream(ssc, None)

    val statuses = rawTweets.map(status => status.getText)

    val words = statuses.flatMap(text => text.split(" "))

    val hashtags = words.filter(word => word.startsWith("#"))

    val hashtagKeyValues = hashtags.map(hashtag => (hashtag, 1))

    val hashtagCounts = hashtagKeyValues.reduceByKeyAndWindow(_+_,_-_, duration, duration)

    val sortedResults = hashtagCounts.transform(rdd => rdd.sortBy(x => x._2, ascending = false))

    def filterResults(stream: (String,Int)): String = {
      val streamString = stream.toString
      val sub = streamString.substring(2, streamString.lastIndexOf(","))
      val num = streamString.substring(streamString.lastIndexOf(",") + 1, streamString.length - 1)
      val result = sub + "," + num
      result
    }

    Logger.getRootLogger.setLevel(Level.ERROR)

    val filteredResults = sortedResults.map(filterResults)

    filteredResults.foreachRDD(rdd => rdd.coalesce(1).saveAsTextFile("Results"))
    
    // Set a checkpoint directory, and kick it all off
    ssc.checkpoint("Checkpoint")

    ssc.start()
    ssc.awaitTerminationOrTimeout(duration.milliseconds)
    ssc.stop()

    val now = Calendar.getInstance.getTime.toString.replace(" ", "-").replace(":", ".")
    val writer = new PrintWriter(s"results-$now.csv")
    val lines = scala.io.Source.fromFile("Results/part-00000").getLines()
    for (line <- lines) {
      if (line.contains(",") && !line(0).equals(",")) {
        writer.append(line)
        writer.println
      }
    }
    writer.close
  }  
}
