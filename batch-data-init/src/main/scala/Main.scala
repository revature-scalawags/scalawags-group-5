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
      var minute = ""

      while(i < args.length){
        args(i) match {
          case "-y" => year = args(i + 1)
          case "-m" => month = formatStringDate( args(i + 1) )
          case "-d" => day = formatStringDate( args(i + 1) )
          case "-min" => minute = formatStringDate( args(i + 1) )
        }

        i += 2
      }

      if(args.length == 8){
        One_hour_fixTwitterJson(year, month, day, minute)
      }else{
        fixTwitterJson(year, month, day)
      }

    }else if(args.length > 0){
      println("Must have at least if not the default: Month, Day, and Year.")
      println("-y\tFollowed by Year:Year of the file.")
      println("-m\tFollowed by Month:Month of the file.")
      println("-d\tFollowed by Day:Day of the file.")
      println("-min\tFollowed by Minute:The particular minute of the file.")

    }else{
      //fixTwitterJson("2020", "03", "01")
      One_hour_fixTwitterJson("2020", "03", "01", "00")
    }

    
  }

  def One_hour_fixTwitterJson(year: String, month: String, day: String, hour: String):Unit = {

    val fileName = s"${year}_${month}_${day}_${hour}.json"
    val file = new File(fileName)
    val myWriter = new PrintWriter(file)

    myWriter.write("[")

    for(j <- 0 until 1){

      val folderNumber = hour
      val fileNumber: String = formatStringDate( j.toString() )
      val sourcepath = s"twitter_stream_${year}_${month}_${day}/${month}/${day}/$folderNumber/$fileNumber.json"
      val lines = scala.io.Source.fromFile(sourcepath).getLines()

      for(line <- lines){
        if(lines.hasNext){
          myWriter.write(line + ",\n")
        }else{
          myWriter.write(line)
        }
      }

    }

    myWriter.write("]")
    myWriter.close()
  }

  def fixTwitterJson(year: String, month: String, day: String):Unit = {

    val fileName = s"${year}_${month}_${day}.json"
    val file = new File(fileName)
    val myWriter = new PrintWriter(file)

    myWriter.write("[")
    
    for(i <- 0 until 24){
      val hour: String = formatStringDate( i.toString() )

      for(j <- 0 until 60){
        
        val minute: String = formatStringDate( j.toString() )

        val sourcepath = s"twitter_stream_${year}_${month}_${day}/${month}/${day}/${hour}/${minute}.json"
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

  def formatStringDate(date: String):String = {
    if(date.length() < 2){
      return formatStringDate("0" + date)
    }else{
      return date
    }
  }

}