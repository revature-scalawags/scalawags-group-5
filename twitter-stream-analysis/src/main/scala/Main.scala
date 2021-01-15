import org.apache.commons.lang.StringUtils.{split, trim}
import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import awscala._
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3Client, AmazonS3ClientBuilder}
import s3._

import java.io._
import java.util.Calendar
import scala.collection.mutable.ArrayBuffer
import com.amazonaws.services.s3.AmazonS3

object Main  {

    def lineCheck(line: String): Boolean = {
        if (line.length == 0) false
        else if (String.valueOf(line(0)).equals(",") || String.valueOf(line(0)).equals(" ")) false
        else if (!line.contains(",") || !line(line.length-1).isDigit) false
        else true
    }

    def removeExtraHash (line: String): String = {
        if (String.valueOf(line(0)).equals("#")) line.substring(1)
        else line
    }

    def getTopCount(x: String, y: String): String = {
        val xNum = x.substring(x.lastIndexOf(",") + 1).toInt
        val yNum = y.substring(y.lastIndexOf(",") + 1).toInt
        if (xNum > yNum) return x
        else if (yNum > xNum) return y
        else return x + " " + y
    }

    def letterFilter(rdd: RDD[String], letter: Char): String = {
        val filteredRDD = rdd.filter(line => line.startsWith(String.valueOf(letter).toLowerCase) || line.startsWith(String.valueOf(letter).toUpperCase))
        if (filteredRDD.isEmpty) return s"${letter.toUpper}: 0"
        else return s"${letter.toUpper}: " + filteredRDD.reduce(getTopCount)
    }
    def formatForFile (line: String): String = {
        val splitLine = line.split(" ").slice(1, line.length)
        val cutOut = splitLine.map(x => x.substring(0, x.lastIndexOf(",")))
        val items = cutOut.mkString(" ")
        val result = line(0) + "," + line.substring(line.lastIndexOf(",") + 1) + "," + items
        result
    }
    def formatForConsole (line: String): String = {
        val hits = line(line.length - 1)
        val splitLine = line.split(" ").slice(1, line.length)
        val cutOut = splitLine.map(x => x.substring(0, x.lastIndexOf(",")))
        val ht = if (cutOut.length > 1) "Hashtags" else "Hashtag"
        val items = cutOut.mkString(", ")
        val spaces = " " * (200 - items.length)
        val result = s"Letter: ${line(0)} \t Hits: $hits \t Top $ht: $items"
        result
    }

    def saveAsFile(fileArray: Array[String], path: String): File = {
        val writer = new PrintWriter(path)
        for (i<-fileArray) writer.append(i + "\n")
        writer.close()
        return new File(path)
    }

    def getS3Client(key: String, secret: String): AmazonS3 = {
        val credentials = new BasicAWSCredentials(key, secret)
        val client = AmazonS3ClientBuilder.standard()
          .withCredentials(new AWSStaticCredentialsProvider(credentials))
          .withRegion("us-east-1")
          .build();
          client
    }

    // def storeInBucket(file: File, newFileName: String, path: String, key: String, secret: String): Unit = {
    //     val credentials = new BasicAWSCredentials(key, secret)
    //     val client = AmazonS3ClientBuilder.standard()
    //       .withCredentials(new AWSStaticCredentialsProvider(credentials))
    //       .withRegion("us-east-1")
    //       .build();
    //     client.putObject(path, newFileName, file)
    // }

    def main(args: Array[String]): Unit = {
        val key = sys.env.get("AWS_ACCESS_KEY_ID").get
        val secret = sys.env.get("AWS_SECRET_ACCESS_KEY").get
        val client = getS3Client(key, secret)
        val sc = new SparkContext("local[*]", "TwitterStreamAnalysis")
        val input = sc.textFile(s"s3a://cpiazza01-revature/project2/Results/part-00000")
          .filter(lineCheck)
          .map(removeExtraHash).cache()

        Logger.getRootLogger.setLevel(Level.ERROR)

        val results = new Array[String](26)

        for (i <- 0 until results.length) {
            results(i) = letterFilter(input, ('a' + i).toChar)
        }

        val fileResults = results.map(formatForFile)
        val consoleResults = results.map(formatForConsole)
        consoleResults.foreach(println)
        fileResults.foreach(println)
        sc.stop()
        val now = Calendar.getInstance().getTime.toString.replace(" ", "-").replace(":", ".")
        val file = saveAsFile(fileResults, s"FinalResults/HashtagStreamResults-$now.csv")
        val s3Path = "cpiazza01-revature/project2/finalStreamingResults"
        //storeInBucket(file, file.getName, s3Path, key, secret)
        client.putObject(s3Path, file.getName, file)
    }
}