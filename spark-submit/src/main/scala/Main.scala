import org.apache.spark.sql.{SparkSession, Dataset}
import org.apache.spark.sql.functions
import org.apache.spark.sql.functions._
import scala.collection.mutable.{Map, WrappedArray, ListMap}
import scala.util.control.Breaks._
import scala.io.Source
import java.io._
import javax.xml.transform.Source
import org.apache.spark.sql.Row

object Main  {

  def main(args: Array[String]) {
    if(args.length == 1 || args.length == 2){
      val spark = SparkSession.builder().appName("spark-test").getOrCreate()
      spark.sparkContext.setLogLevel("WARN")
      init(spark, args)
      spark.stop()
    }else{
      println("Please enter in correct arguments.")
      println("Folder file")
      println("Folder")
    }
  }

  /** Reads in the Json file, does queries on the data, and then writes the data to a file.
    * @param spark The Spark session of the app.
    */
  def init(spark: SparkSession, args: Array[String]) {
    import spark.implicits._

    var path = args(0) 
    if(args.length == 2){ 
      path += "/" + args(1) 
    }

    // Import the file from the datalake
    val jsonfile = spark.read.option("multiline", "true").json("/datalake/" + path).cache()

    // Print the schema of the Json File
    //jsonfile.printSchema()

    // Reading the data into a dataset of just data that is needed.
    val tweetDataset = jsonfile.as[Tweet]

    // Different way of doing spark sql
    //tweetDataset.createOrReplaceTempView("TweetData")
    //spark.sql("SELECT * FROM TweetData")

    // Queries
    val sourceCounting = SourceQuery(tweetDataset)
    val textCounting = TextQuery(tweetDataset)
    val verifiedCounting = VerifiedQuery(tweetDataset)
    val hashtagCounting = HashtagsQuery(tweetDataset)
    val langCounting = LangQuery(tweetDataset)

    var fileStart = ""

    if(args.length == 2){
      fileStart = args(1) + "_"
    }

    // Write to the datawarehouse
    fileWriter( args(0) + "/" + fileStart + "source.csv", sourceCounting)
    fileWriter( args(0) + "/" + fileStart + "verified.csv", verifiedCounting)
    fileWriter( args(0) + "/" + fileStart + "text.csv", textCounting)
    fileWriter( args(0) + "/" + fileStart + "hashtag.csv", hashtagCounting)
    fileWriter( args(0) + "/" + fileStart + "lang.csv", langCounting)

    // Write to the CLI
    writeCLI(sourceCounting, "Source", 10)
    writeCLI(verifiedCounting, "Verified", 10)
    writeCLI(textCounting, "Text", 50)
    writeCLI(hashtagCounting, "Hashtag", 50)
    writeCLI(langCounting, "Language", 10)
    
  }

   /** Counts the total occurances of tweets coming from Android phones, iOS phones, and from a personal computer's web browser
     * @param tweetDataset Dataset containing Twitter data
     * @return Returns a map containing the category of the tweet (Android, iPhone, PC) and the number of times this type of tweet occured
     */
  def SourceQuery(tweetDataset: Dataset[Tweet]):Map[String, Int] = {
    //println("Source")

    // SQL query on the dataset
    val sqlquery = tweetDataset.groupBy("source").count().collect()
    var sourceCounting = scala.collection.mutable.Map[String, Int]("android" -> 0, "ios" -> 0, "web" -> 0)

    // For each row in the query
    for (index <- sqlquery){

      // If there is a source
      if (index(0) != null){
        var sourceUrl = index(0).toString().toLowerCase() 

        // Not reliable data but this is the little checking one can do to tell what source it is 
        index(0).toString().toLowerCase() match {
          case x if x.contains("ios") || x.contains("ipad") || x.contains("iphone") || x.contains("iοs") => sourceCounting("ios") += index(1).toString().toInt
          case x if x.contains("android") || x.contains("smartphone") => sourceCounting("android") += index(1).toString().toInt
          case x => sourceCounting("web") += index(1).toString().toInt
        }

      }
    }

    //sourceCounting.foreach( x => println(x._1 + ": " + x._2)  )
    sourceCounting
  }

  /** Queries the dataset based on the text included in the tweets
    * Breaks up text based on english characters and returns map of word occurences
    * @param tweetDataset the data of each tweet in the source
    * @return map of each word found and the occurences of them
    */
  def TextQuery(tweetDataset: Dataset[Tweet]):Map[String, Int] = {
    //println("Text")

    // SQL query on the dataset
    val sqlquery = tweetDataset.select("text").collect()
    var textCounting = Map[String, Int]()

    var reg = "[^a-zA-Z0-9\']".r

    // For each row in the query
    for (index <- sqlquery){

      if(index(0) != null){
        var words = reg.replaceAllIn(index(0).toString().toLowerCase(), " ").split(" ").filter(_.nonEmpty)

        words.foreach( x => {
          if(textCounting.contains(x)){
            textCounting(x) += 1
          }else{
            textCounting += (x -> 1)
          }
        })

      }
    }

    // DELETE THIS
    //val printable = textCounting.toSeq.sortWith(_._1 > _._1)
    
    //textCounting.foreach( x => if(x._2 > 1){ println(x._1 + ": " + x._2) } )
    textCounting
  }

    /** Counts the total occurances of tweets coming from verified users and unverified users
     * @param tweetDataset Dataset containing Twitter data
     * @return Returns a map containing the category of the tweet for this specific scenario (verified/unverified) and the number of times this type of tweet occured
     */
  def VerifiedQuery(tweetDataset: Dataset[Tweet]):Map[String, Int] = {
    //println("Verified")

    // SQL query on the dataset
    val sqlquery = tweetDataset.groupBy("user.verified").count().collect()

    //tweetDataset.groupBy("user.verified").count().sort("count").show(10)
    //tweetDataset.groupBy("user.verified").count().withColumnRenamed("count", "n").sort("n").limit(5).printSchema()

    val verifiedCounting = scala.collection.mutable.Map[String, Int]()

    // For each row in the query
    for (index <- sqlquery){
      if (index(0) != null){
        verifiedCounting += (index(0).toString() -> index(1).toString().toInt)
      }
    }

    //verifiedCounting.foreach( x => println(x._1 + ": " + x._2)  )
    verifiedCounting
  }

    /** Counts the total occurances of hashtags in all Tweets in our dataset
     * @param tweetDataset Dataset containing Twitter data
     * @return Returns a map containing hashtag names and number of times each hashtag occurred in the dataset
     */
  def HashtagsQuery(tweetDataset: Dataset[Tweet]):Map[String, Int] = {
    //println("Hashtag")

    // SQL query on the dataset
    val sqlquery = tweetDataset.select("entities.hashtags.text").collect()
    var hashtagCounting = Map[String, Int]()

    // For each row in the query
    for (index <- sqlquery){

      if (index(0) != null){

        // This is how the Dataset saved the array of hashtags strings
        var hashtagWrappedArray: WrappedArray[String] = index(0).asInstanceOf[WrappedArray[String]]
        var hashtagArray = hashtagWrappedArray.toArray

        // Count each hashtag
        hashtagArray.foreach( x => {
          if(hashtagCounting.contains(x)){
            hashtagCounting(x) += 1
          }else{
            hashtagCounting += (x -> 1)
          }
        })

      }
    }

    //hashtagCounting.foreach( x => println(x._1 + ": " + x._2)  )
    hashtagCounting
  }

  /** Counts the total occurances of languages used for tweets in our dataset
   * @param tweetDataset Dataset containing Twitter data
   * @return Returns a map containing the languages used for the tweets in our dataset, along with a count of how many times each language occurs
   */
  def LangQuery(tweetDataset: Dataset[Tweet]):Map[String, Int] = {
    //println("Language")

    // SQL query on the dataset
    val sqlquery = tweetDataset.groupBy("lang").count().collect()
    //tweetDataset.groupBy("lang").count().filter("lang != 'ind'").sort(desc("count")).show()

    var langCounting = Map[String, Int]()

    // For each row in the query
    for (index <- sqlquery){

      // Check if there is a language then count
      if(index(0) != null){
        langCounting += (index(0).toString() -> index(1).toString().toInt)
      }

    }

    //langCounting.foreach( x => println(x._1 + ": " + x._2)  )
    langCounting
  }
  
  /** Writes information passed to the datawarehouse
    * @param filename the name of the file written to the datawarehouse
    * @param Counting the analysis that is to be written to the file
    */
  def fileWriter(fileName: String, counting: Map[String, Int]):Unit = {
    val writer = new PrintWriter( new File("datawarehouse/" + fileName) )

    counting.foreach( x => {
      writer.write(x._1 + ", " + x._2 + "\n")
    })

  }

  /** Simply prints the results of our analyses to the console for viewing.
    * 
    * @param counting The output from our analysis
    * @param name name of analysis being run
    * @param numberReturns Number of results printed to CLI
    */
  def writeCLI(counting: Map[String, Int], name: String, numberReturns: Int):Unit = {
    println(name)
    
    // Sort the map
    val sortedList = counting.toSeq.sortWith(_._2 > _._2)
  
    var i = 1
    breakable{
      for(i <- 0 until numberReturns){
        if(i >= sortedList.size){
          break
        }

        println( (i + 1) + ". " + sortedList(i)._1 + ": " + sortedList(i)._2)

      }
    }

    println("")

  }

  // Case classes for the dataset formed from the json of twitter data
  case class Tweet(source: String, text: String, user: User, entities: Entities, lang: String)
  case class User(verified: Boolean)
  case class Entities(hashtags: Array[Text])
  case class Text(text: String)
}