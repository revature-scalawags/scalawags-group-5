import org.apache.spark._
import com.amazonaws.auth.{BasicAWSCredentials, AWSStaticCredentialsProvider}
import com.amazonaws.services.s3.{AmazonS3ClientBuilder, AmazonS3}

object Main {
    def getS3Client(key: String, secret: String): AmazonS3 = {
        val credentials = new BasicAWSCredentials(key, secret)
        val client = AmazonS3ClientBuilder.standard()
          .withCredentials(new AWSStaticCredentialsProvider(credentials))
          .withRegion("us-east-1")
          .build();
          client
    }

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

    def backToString(stream: (String,Int)): String = {
        val streamString = stream.toString
        val result = streamString.substring(1, streamString.length - 1)
        result
    }

    def main (args: Array[String]): Unit = {
        //val key = sys.env.get("AWS_ACCESS_KEY_ID").get
        //val secret = sys.env.get("AWS_SECRET_ACCESS_KEY").get
        //val client = getS3Client(key, secret)
        val sc = new SparkContext("local[*]", "FileMerger")

        val data = sc.textFile("../twitter-stream/DataChunks/1-14/*")
        .filter(lineCheck)
        .map(removeExtraHash)
        .map(line => (line.substring(0, line.lastIndexOf(",")), line.substring(line.lastIndexOf(",") + 1).toInt))
        .reduceByKey((x, y) => x + y)
        .sortBy(x => x._2, ascending = false)
        .map(backToString)

        //data.coalesce(1).saveAsTextFile(s"s3a://cpiazza01-revature/project2/Results")
        data.coalesce(1).saveAsTextFile("FileMergeResults")
        sc.stop()
    }
}