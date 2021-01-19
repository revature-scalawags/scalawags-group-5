import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.log4j.{Level, Logger}
import com.amazonaws.auth.{AWSStaticCredentialsProvider, BasicAWSCredentials}
import com.amazonaws.services.s3.{AmazonS3ClientBuilder}
import java.io._
import java.util.Calendar
import com.amazonaws.services.s3.AmazonS3

object Main {

  def main(args: Array[String]): Unit = {
    println("Welcome to Group Five's Streaming Analysis!")
    Thread.sleep(2000)
    println(
      "This program will analyze the counts of Hashtags streamed in from Twitter over the past few days in 12 hour intervals, and save them to a bucket on S3"
    )
    Thread.sleep(3000)
    println
    val key = System.getenv("AWS_ACCESS_KEY_ID")
    val secret = System.getenv("AWS_SECRET_ACCESS_KEY")
    val client = getS3Client(key, secret)
    val sc = new SparkContext("local[*]", "TwitterStreamAnalysis")

    // Grab hashtag data and filter it into a more useful format
    val input = sc
      .textFile(s"s3a://cpiazza01-revature/project2/Results/SavedData/${args(0)}")
      .filter(lineCheck)
      .map(lineFormat)
      .cache()

    // Set logging level to ERROR so console results will be properly displayed
    Logger.getRootLogger.setLevel(Level.ERROR)

    // Placeholder array for each letter's result
    val results = new Array[String](26)

    // Fill results array with top results for each letter
    for (i <- 0 until results.length) {
      results(i) = letterMap(input, ('a' + i).toChar)
    }

    // Format array for file output
    val fileResults = results.map(formatForFile)

    // Format array for console output
    val consoleResults = results.map(formatForConsole)

    // Print results to console
    consoleResults.foreach(println)

    sc.stop()

    // Get current date/time to use as part of file name for results. This ensures a unique file name every time.
    val now = Calendar
      .getInstance()
      .getTime
      .toString
      .replace(" ", "-")
      .replace(":", ".")

    // Save file locally
    val file = saveAsFile(fileResults, s"FinalResults/HashtagStreamResults-${args(0)}-$now.csv")

    // Define S3 path
    val s3Path  = "cpiazza01-revature/project2/finalStreamingResults"

    // Grab local file and place it on S3 bucket
    client.putObject(s3Path, file.getName, file)
  }

  /** Checks each line of input to ensure it properly encapsulates a hashtag and its relevant count, based on the following criteria:
    * - Length must be five or greater 
    * - Must contain open parenthese, a hashtag, a comma, a number, and close parentheses)
    * @param line Input string from your RDD String
    * @return Returns true if line is valid, otherwise returns false, causing the filter method to discard it
    */
  def lineCheck(line: String): Boolean = {
    if (line.length < 5) false
    else if (
      !String.valueOf(line(0)).equals("(") ||
      !String.valueOf(line(line.length - 1)).equals(")") ||
      !String.valueOf(line(1)).equals("#") ||
      !line.contains(",") ||
      !line(line.length - 2).isDigit
    ) false
    else true
  }

  /** Removes outer parenthesis and hashtag
    * @param line Input string from your RDD String
    * @return Returns a new string stripped of outer parenthese and hashtag
    */
  def lineFormat(line: String): String = {
    val newLine = line.substring(2, line.length - 1)
    newLine
  }

  /** - Meant to be used in a reduce function
    * - Returns the highest occuring hashtag from the input RDD (Number to be compared is at the end of the input strings)
    * @param x First String to be compared
    * @param y Second String to be compared
    * @return Returns the String with the highest amount of hashtag counts, or both if they are tied
    */
  def getTopCount(x: String, y: String): String = {
    val xNum = x.substring(x.lastIndexOf(",") + 1).toInt
    val yNum = y.substring(y.lastIndexOf(",") + 1).toInt
    if (xNum > yNum) return x
    else if (yNum > xNum) return y
    else return x + " " + y
  }

  /** Filters out items in a String RDD that do not start with the specified letter,
    * then calls the 'getTopCount' method to reduce this RDD to the one with the highest count, if one exists
    * @param rdd The String RDD that will be mapped
    * @param letter The letter that each line in the RDD must start with, or else it will be filtered out
    * @return Returns the String with the highest count for the specified letter, if one exists. 
    * If one does not exist, returns a string in the following format:
    * - "[input letter]: 0"
    */
  def letterMap(rdd: RDD[String], letter: Char): String = {
    val filteredRDD = rdd.filter(line =>
      line.startsWith(String.valueOf(letter).toLowerCase) || line.startsWith(
        String.valueOf(letter).toUpperCase
      )
    )
    if (filteredRDD.isEmpty) return s"${letter.toUpper}: 0"
    else return s"${letter.toUpper}: " + filteredRDD.reduce(getTopCount)
  }

  /** Formats the input String to a readable csv format so it can be saved to a file
    * @param line Input String from a String Array
    * @return Returns the input String outputted in CSV format
    */
  def formatForFile(line: String): String = {
    val splitLine = line.split(" ").slice(1, line.length)
    val cutOut =
      if (splitLine(0).length != 1)
        splitLine.map(x => x.substring(0, x.lastIndexOf(",")))
      else Array[String]("")
    val items = cutOut.mkString(" ")
    val result =
      if (items.equals("")) line(0) + ",0,"
      else
        line(0) + "," + line.substring(line.lastIndexOf(",") + 1) + "," + items
    result
  }

  /** Formats the input String to a more readable format so it can be printed to the console
    * @param line Input String from a String Array
    * @return Returns the input String outputted in a more readable format
    */
  def formatForConsole(line: String): String = {
    val hits =
      if (line.indexOf(",") != -1) line.substring(line.lastIndexOf(",") + 1)
      else "0"
    val splitLine = line.split(" ").slice(1, line.length)
    val cutOut =
      if (splitLine(0).length != 1)
        splitLine.map(x => x.substring(0, x.lastIndexOf(",")))
      else Array[String]("")
    val ht = if (cutOut.length > 1) "Hashtags" else "Hashtag"
    val items = cutOut.mkString(", ")
    val spaces = " " * (200 - items.length)
    val result = s"Letter: ${line(0)} \t Hits: $hits \t Top $ht: $items"
    result
  }

  /** Takes an array of strings and writes them to the specified file.
    * Writes each string in the array as a line in the file
    * @param stringArray String array containing lines to be written
    * @param path Path that will be used to save the file. Must declare file name and type in addition to directory path
    * - Example: your/directory/path/filename.csv
    * @return Returns the file, which has been written to and saved on the specified path
    */
  def saveAsFile(stringArray: Array[String], path: String): File = {
    val writer = new PrintWriter(path)
    for (i <- stringArray) writer.append(i + "\n")
    writer.close()
    return new File(path)
  }

  /** Creates an instance of an Amazon S3 client, which can be used to interact with S3 buckets
    * @param key AWS S3 bucket access key ID
    * @param secret AWS S3 bucket secret access key
    * @return Returns a new instance of an Amazon S3 client
    */
  def getS3Client(key: String, secret: String): AmazonS3 = {
    val credentials = new BasicAWSCredentials(key, secret)
    val client = AmazonS3ClientBuilder
      .standard()
      .withCredentials(new AWSStaticCredentialsProvider(credentials))
      .withRegion("us-east-1")
      .build();
    client
  }
}