import java.util.TimerTask
import java.util.Timer
import java.io.{File,FileInputStream,FileOutputStream}
import java.time.LocalDate
import java.time.LocalTime
import java.io.FileReader
import scala.concurrent.duration._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.reflect.io.Directory
import java.util.Calendar

/**
  * Copies the data file on S3 every 12 hours and saves it based on the current time
  * This way, as long as the stream runs alongside this program, 12 hour segments of our streaming data will be automatically generated for us
  */
object saveData extends TimerTask with App {
    override def run() {
        val key = System.getenv("AWS_ACCESS_KEY_ID")
        val secret = System.getenv("AWS_SECRET_ACCESS_KEY")
        val client = Main.getS3Client(key, secret)
        val date = LocalDate.now
        val interval = if (LocalTime.now().isAfter(LocalTime.NOON)) "evening" else "morning"
        val now = Calendar.getInstance.getTime().toString().replace(" ", "-").replace(":", ".")
        val s3SourcePath = "cpiazza01-revature/project2/Results"
        val s3DestPath = "cpiazza01-revature/project2/Results/Saved Data"

        client.copyObject(s3SourcePath, "part-00000", s3DestPath, s"$date-$interval")
        println(s"file copied at: $now")
    }

    val timer = new Timer
    timer.schedule(saveData, 0, Duration(12, HOURS).toMillis)
}