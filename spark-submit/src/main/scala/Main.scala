import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import scala.io.Source
import java.io._
import javax.xml.transform.Source

object Main  {
  def main(args: Array[String]) {
    // val spark = SparkSession.builder().appName("hello world").getOrCreate()
    // spark.sparkContext.setLogLevel("WARN")
    // helloSQL(spark)
    // spark.stop()
    fixTwitterJson()
  }

  def helloSQL(spark: SparkSession) {
    import spark.implicits._
    val jsonfile = spark.read.option("multiline", "true").json("/datalake/people.json").cache()
    jsonfile.show()
    jsonfile.printSchema()
    jsonfile.select($"name", $"age" + 10).show()
    jsonfile.select(jsonfile("name.first"), jsonfile("age") + 20).show()
    jsonfile.groupBy("eyeColor").count().show()
    jsonfile.select(functions.round($"age", -1)).show()



    // What is the average age by eye color for people with first names of length < 6
    val weirdQuery = jsonfile.filter(functions.length($"name.first") < 6)
      .groupBy("eyeColor")
      .agg(functions.avg("age"))

    weirdQuery.show()
    weirdQuery.explain(true)



    val peopleDataSet = jsonfile.as[Person]
    peopleDataSet.filter(person => person.name.first.length < 6).show()

    val weirdQuery2 = peopleDataSet.filter(_.name.first.length < 6)
      .map(person => s"${person.name.first} ${person.name.last}")

    weirdQuery2.select(weirdQuery2("value").alias("Full Name")).show()
    


    jsonfile.write.partitionBy("eyeColor").parquet("/datawarehouse/people.parquet")



    val peopleParquet = spark.read.parquet("/datawarehouse/people.parquet")
    peopleParquet.show()



    spark.createDataset(List(Name("Mehrab", "Rahman"), Name("Rahman", "Mehrab")))
      .createOrReplaceTempView("names")
    
    println(spark.sql("SELECT * FROM names").rdd.toDebugString)

  }

  def One_hour_fixTwitterJson(hour: String):Unit = {
    val fileName = s"0301_$hour.json"
    val file = new File(fileName)
    // if(!file.exists){
    //   new java.io.File(fileName)
    // }
    val myWriter = new PrintWriter(file)
    myWriter.write("[")
    for(j <- 0 until 60){
      val folderNumber = hour
      val fileNumber: String = if(j < 10){ "0" + j }else{ j.toString() }
      val sourcepath = s"twitter_example/twitter_stream_2020_03_01/03/01/$folderNumber/$fileNumber.json"
      val lines = scala.io.Source.fromFile(sourcepath).getLines()
      for(line <- lines){
        if(lines.hasNext){
          myWriter.write(line + ",")
        }else{
          myWriter.write(line)
        }
      }
    }
    myWriter.write("]")
    myWriter.close()
  }

  def fixTwitterJson():Unit = {
    val fileName = "all0301.json"
    val file = new File(fileName)
    // if(!file.exists){
    //   new java.io.File(fileName)
    // }
    val myWriter = new PrintWriter(file)
    myWriter.write("[")
    for(i <- 0 until 24){
      for(j <- 0 until 60){
        val folderNumber: String = if(i < 10){ "0" + i }else{ i.toString() }
        val fileNumber: String = if(j < 10){ "0" + j }else{ j.toString() }
        val sourcepath = s"twitter_example/twitter_stream_2020_03_01/03/01/$folderNumber/$fileNumber.json"
        val lines = scala.io.Source.fromFile(sourcepath).getLines()
        for(line <- lines){
          if(lines.hasNext){
            myWriter.write(line + ",")
          }else{
            myWriter.write(line)
          }
        }
      }
    }
    myWriter.write("]")
    myWriter.close()
  }

  def helloWorld(spark: SparkSession) {
    import spark.implicits._
    val file = spark.read.textFile("/tmp/logfile.txt").cache()
    val split = file.map(line => line.split(","))
    split.foreach(i => println(i))
  }

  case class Person(_id: String, index: Long, age: Long, eyeColor: String, name: Name, phone: String, address: String) {}
  case class Name(first: String, last: String) {}
}