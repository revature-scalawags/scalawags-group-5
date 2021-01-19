import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.Dataset
import scala.collection.mutable.Map
import scala.collection.mutable.WrappedArray
import scala.io.Source
import java.io._
import javax.xml.transform.Source
import org.apache.spark.sql.Row


/** 
  * 
  */
object Main  {

  /** 
    * 
    * 
    * @param args
    */
  def main(args: Array[String]) {
    val spark = SparkSession.builder().appName("spark-test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    init(spark)
    spark.stop()
  }

  /** Reads in the Json file, does queries on the data, and then writes the data to a file.
    * 
    *
    * @param spark The Spark session of the app.
    */
  def init(spark: SparkSession) {
    import spark.implicits._

    // Import the file from the datalake
    val jsonfile = spark.read.option("multiline", "true").json("/datalake/month").cache()

    // Print the schema of the Json File
    //jsonfile.printSchema()

    // Reading the data into a dataset of just data that is needed.
    val tweetDataset = jsonfile.as[Tweet]

    // Different way of doing spark sql
    //tweetDataset.createOrReplaceTempView("TweetData")
    //spark.sql("SELECT * FROM TweetData")

    // Queries
    //var sourceCounting = SourceQuery(tweetDataset)
    var textCounting = TextQuery(tweetDataset)
    //var verifiedCounting = VerifiedQuery(tweetDataset)
    //var hashtagCounting = HashtagsQuery(tweetDataset)
    //var langCounting = LangQuery(tweetDataset)

    // Write to the datawarehouse
    //val writer = new PrintWriter( new File("datawarehouse/text.csv") )
    // FileWritiing(writer, sourceCounting)
    
  }

  // Done - Might want to check??? this isn't reliable data
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

        // index(0).toString().toLowerCase() match {
        //   case x if x.contains("ios") || x.contains("ipad") || x.contains("iphone") || x.contains("iοs") => println("IOS: " + sourceUrl)
        //   case x if x.contains("android") => println("ANDROID: " + sourceUrl)
        //   case x => println("WEB: " + x)
        // }
        
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

  // Not Done
  def TextQuery(tweetDataset: Dataset[Tweet]):Map[String, Int] = {
    println("Text")

    // SQL query on the dataset
    val sqlquery = tweetDataset.select("text").collect()
    var textCounting = Map[String, Int]()

    var reg = "[^a-zA-Z0-9\\\']".r

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
      //TODO: Make a word counter
      //println(index)
      //println(index(0)) // This is the text
    }

    textCounting.foreach( x => println(x._1 + ": " + x._2)  )
    textCounting
  }

  // Done
  // *** null is still in this ***
  def VerifiedQuery(tweetDataset: Dataset[Tweet]):Map[String, Int] = {
    //println("Verified")

    // SQL query on the dataset
    val sqlquery = tweetDataset.groupBy("user.verified").count().collect()
    val verifiedCounting = scala.collection.mutable.Map[String, Int]()

    // For each row in the query
    for (index <- sqlquery){
      if (index(0) != null){
        verifiedCounting += (index(0).toString() -> index(1).toString().toInt)
      }else{
        verifiedCounting += ("null" -> index(1).toString().toInt)
      }
    }

    //verifiedCounting.foreach( x => println(x._1 + ": " + x._2)  )
    verifiedCounting
  }

  // Done
  // *** null is still in this ***
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

      }else{

        // Might not have this data it is essential how many tweets don't have any hashtags
        if(hashtagCounting.contains("null")){
          hashtagCounting("null") += 1
        }else{
          hashtagCounting += ("null" -> 1)
        }
        
      }
    }

    //hashtagCounting.foreach( x => println(x._1 + ": " + x._2)  )
    hashtagCounting
  }

  // Done
  // *** There are null (couldn't figure it out) and und (und means undefined) are still in this ***
  def LangQuery(tweetDataset: Dataset[Tweet]):Map[String, Int] = {
    //println("Language")

    // SQL query on the dataset
    val sqlquery = tweetDataset.groupBy("lang").count().collect()
    var langCounting = Map[String, Int]()

    // For each row in the query
    for (index <- sqlquery){

      // Check if there is a language then count
      if(index(0) != null){
        langCounting += (index(0).toString() -> index(1).toString().toInt)
      }else{
        langCounting += ("null" -> index(1).toString().toInt)
      }

    }

    //langCounting.foreach( x => println(x._1 + ": " + x._2)  )
    langCounting
  }

  // Case classes for the dataset formed from the json of twitter data
  case class Tweet(source: String, text: String, user: User, entities: Entities, lang: String)
  case class User(verified: Boolean)
  case class Entities(hashtags: Array[Text])
  case class Text(text: String)
}