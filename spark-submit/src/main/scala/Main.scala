import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.Dataset
import scala.collection.mutable.Map
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

    // Queries
    var sourceCounting = SourceQuery(tweetDataset)
    var textCounting = TextQuery(tweetDataset)
    var verifiedCounting = VerifiedQuery(tweetDataset)
    var hashtagCounting = HashtagsQuery(tweetDataset)
    var langCounting = LangQuery(tweetDataset)

    // Write to the datawarehouse
    //val writer = new PrintWriter( new File("datawarehouse/text.csv") )
    // FileWritiing(writer, sourceCounting)
    
  }

  // Not Done - test
  def SourceQuery(tweetDataset: Dataset[Tweet]):Map[String, Int] = {
    println("Source")

    val sqlquery = tweetDataset.groupBy("user.verified").count()
    var sourceCounting = Map[String, Int]("Android" -> 0, "IOS" -> 0, "Web" -> 0)

    for (index <- sqlquery){

      // index(0).toString() match {
      //   case index.toString().contains("android") => println(index(0))
      // }

      if (index(0) != null){
        var sourceUrl = index(0).toString().toLowerCase() 
        println(sourceUrl)
        
        // sourceUrl match {
        //   case sourceUrl contains "iso" => println(sourceUrl)
        //   case sourceUrl contains "android" => println(sourceUrl)
        //   case _ => println()
          
        // }
      }

      

      println(index(0)) // This is the url
      println(index(1)) // This is the count.
    }

    sourceCounting
  }

  

  // Not Done
  def TextQuery(tweetDataset: Dataset[Tweet]):Map[String, Int] = {
    println("Text")

    val sqlquery = tweetDataset.select("text")
    var textCounting = Map[String, Int]()

    for (index <- sqlquery){
      println(index)
      println(index(0)) // This is the text
    }

    textCounting
  }

  // Done
  def VerifiedQuery(tweetDataset: Dataset[Tweet]):Map[String, Int] = {
    println("Verified")

    val sqlquery = tweetDataset.groupBy("user.verified").count()
    var verifiedCounting = Map[String, Int]()

    for (index <- sqlquery){
      verifiedCounting += (index(0).toString() -> index(1).toString().toInt)
      println(index(0)) // This is the url
      println(index(1)) // This is the count.
    }

    verifiedCounting.foreach( println )
    verifiedCounting
  }

  // Not Done
  def HashtagsQuery(tweetDataset: Dataset[Tweet]):Map[String, Int] = {
    println("Hashtag")

    val sqlquery = tweetDataset.groupBy("entities.hashtags.text").count()
    var hashtagCounting = Map[String, Int]()

    for (index <- sqlquery){
      println(index)
      println(index(0)) // This is the url
      println(index(1)) // This is the count.
    }

    hashtagCounting.foreach( println )
    hashtagCounting
  }

  // Done? Not tested
  def LangQuery(tweetDataset: Dataset[Tweet]):Map[String, Int] = {
    println("Language")
    val sqlquery = tweetDataset.groupBy("lang").count()
    var langCounting = Map[String, Int]()

    for (index <- sqlquery){
      // check if null
      if(index(0) == null){
        langCounting += (index(0).toString() -> index(1).toString().toInt)
      }else{
        langCounting += (index(0).toString() -> index(1).toString().toInt)
      }
      println(index(0)) // This is the language.
      println(index(1)) // This is the count.
    }

    langCounting.foreach( println )
    langCounting
  }

  case class Tweet(source: String, text: String, user: User, entities: Entities, lang: String)
  case class User(verified: Boolean)
  case class Entities(hashtags: Array[Text])
  case class Text(text: String)
}