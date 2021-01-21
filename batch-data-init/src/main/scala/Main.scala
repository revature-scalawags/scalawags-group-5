import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import scala.io.Source
import java.io._
import javax.xml.transform.Source

object Main {

  def main(args: Array[String]) {

    if(args.length == 6 || args.length == 8){
      var i = 0

      var year = ""
      var month = ""
      var day = ""
      var hour = ""

      while(i < args.length){
        args(i) match {
          case "-y" => year = args(i + 1)
          case "-m" => month = formatStringDate( args(i + 1) )
          case "-d" => day = formatStringDate( args(i + 1) )
          case "-h" => hour = formatStringDate( args(i + 1) )
        }

        i += 2
      }

      if(args.length == 8){
        oneHourFixTwitterJson(year, month, day, hour)
      }else{
        fixTwitterJson(year, month, day)
      }

    //}else if(args.length > 0){
    }else{
      println("Must have at least if not the default: Month, Day, and Year.")
      println("-y\tFollowed by Year:Year of the file.")
      println("-m\tFollowed by Month:Month of the file.")
      println("-d\tFollowed by Day:Day of the file.")
      println("-h\tFollowed by Minute:The particular minute of the file.")

      println("If you want the full day in the month please don't enter in the hour.")
      println("Make sure the setup in the readme is fully done before the program is ran.")

    }

    
  }
  /** Reads in multiple Json files from a specified hour formatting them to be parsed correctly later
    * @param year year of the file given to specify file path
    * @param month month of the file given to specify file path
    * @param day day of the file given to specify file path
    * @param hour hour of the file given to specify file path
    */
  def oneHourFixTwitterJson(year: String, month: String, day: String, hour: String):Unit = {

    val fileName = s"${year}_${month}_${day}_${hour}.json"
    val file = new File(fileName)
    val myWriter = new PrintWriter(file)
    var ugg = 0

    myWriter.write("[")

    for(j <- 0 until 60){

      val fileNumber: String = formatStringDate( j.toString() )
      val sourcepath = s"twitter_stream_${year}_${month}_${day}/${month}/${day}/${hour}/$fileNumber.json"
      val lines = scala.io.Source.fromFile(sourcepath).getLines()

      for(line <- lines){
        if(lines.hasNext && line.substring(2,8) != "delete"){
          if(ugg == 0){
            myWriter.write(line)
            ugg = 1
          }else{
            myWriter.write(",\n" + line)
          }
        }else if(line.substring(2,8) != "delete"){
          myWriter.write(",\n" + line)
        }
      }

    }

    myWriter.write("]")
    myWriter.close()
  }

  /** Reads in multiple Json files from the first minute for a every hour in a day
    * @param year year of the file given to specify file path
    * @param month month of the file given to specify file path
    * @param day day of the file given to specify file path
    */
  def fixTwitterJson(year: String, month: String, day: String):Unit = {

    val fileName = s"${year}_${month}_${day}.json"
    val file = new File(fileName)
    val myWriter = new PrintWriter(file)
    var ugg = 0

    myWriter.write("[")
    
    for(i <- 0 until 24){
      val hour: String = formatStringDate( i.toString() )

      for(j <- 0 until 1){
        
        val minute: String = formatStringDate( j.toString() )

        val sourcepath = s"twitter_stream_${year}_${month}_${day}/${month}/${day}/${hour}/${minute}.json"
        val lines = scala.io.Source.fromFile(sourcepath).getLines()

        for(line <- lines){
          if(lines.hasNext && line.substring(2,8) != "delete"){
            if(ugg == 0){
              myWriter.write(line)
              ugg = 1
            }else{
              myWriter.write(",\n" + line)
            }
          }else if(line.substring(2,8) != "delete"){
            myWriter.write(",\n" + line)
          }
        }

      }
    }
    
    myWriter.write("]")
    myWriter.close()

  }

  /** Formats dates entered to appropriately mirror file paths if the value is under 10
    * @param date a date value such as day, hour or minute
    */
  def formatStringDate(date: String):String = {
    if(date.length() < 2){
      return formatStringDate("0" + date)
    }else{
      return date
    }
  }

}