# TWITTER STREAM USING SPARK STREAMING #

## Project Description ##
Spark application that utilizes Spark Streaming to pull in real-time twitter data, then filter it out based on hashtags and a count of those hashtags.

## Technologies Used ##
- Scala - version 2.12.12
- Spark - version 3.0.1
- Spark Streaming
- twitter4j sbt package

## Features ##
- Captures world-wide incoming tweets in real time
- Filters incoming tweets to only grab hashtags
- Count occurances of each hashtag
- Generates a file on your specified S3 bucket, updating every five minutes, filtering in 12 hour windows

## Usage ##
- Navigate to the "twitter-stream" directory in your shell, then run "sbt assembly"
- Once the jar is generated, it will be located in target/scala-x.xx. Copy this jar over to your spark cluster.
- Save your twitter API credentials in a text file called "twitterProps" on the root directory of the jar on your spark cluster. Use the following format:
```
consumerKey ***************************
consumerSecret ******************************************
accessToken ***************************************
accessTokenSecret ************************************************
```
- Save your AWS S3 credentials as environment variables in your spark cluster shell session in the following format:
```
AWS_ACCESS_KEY_ID=*******************
AWS_SECRET_ACCESS_KEY=**********************************
```
- Run spark-submit on the jar file, specifying "Main" as the class and specifying the path to your S3 bucket (where the results will be streamed to) as the only argument. MAKE SURE THIS PATH IS VALID! For example:

```bash
spark-submit 
--class Main 
--master local[*] 
yourJar.jar 
bucket-name/directory/sub-directory
```

- After ten minutes, a file called "part-00000" will be saved to your specified bucket 
    - This file will be updated every ten minutes with the latest hashtag data
    - Once 12 hours have elapsed, the file will begin filtering out data that is older than 12 hours, while still adding new data
    - As such, once 12 hours have elapsed, your "part-00000" file will contain the previous 12 hours' data for as long as your stream continues to run
- When you want to terminate your stream, enter the following into your terminal (courtesy of [this blogpost](http://why-not-learn-something.blogspot.com/2016/05/apache-spark-streaming-how-to-do.html)): 

```bash
ps -ef | grep spark |  grep [DriverProgramName] | awk '{print $2}'   | xargs kill  -SIGTERM
```