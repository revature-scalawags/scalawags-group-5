# TWITTER STREAM USING DSTREAMS #

## Project Description ##
Spark application that utilize Sparks Dtream object to pull in real-time twitter data, then filter it out based on hashtags and a count of those hashtags.

## Technologies Used ##
- Scala
- Spark
- Spark Dstreams
- twitter4j sbt package

## Features ##
- Captures world-wide incoming tweets in real time
- Filters incoming tweets to only grab hashtags
- Count occurances of each hashtag
- Generates a file once the batch interval has been completed

## Usage ##
- Save your twitter API credentials as environment variables in your shell session.
- Run this on your Spark cluster as a jar file with spark-submit
    - A "Results" folder will be generated in the same directory as the jar file 
    - After one minute, a file called "part-00000" will be saved to this folder
    - This file will be updated every minute with the latest hashtag data
    - Once 24 hours have elapsed, the file will begin filtering out data that is older than 24 hours, while still adding new data
    - As such, once 24 hours have elapsed, your "part-00000" file will contain the previous 24 hours' data for as long as your stream continues to run
- When you want to terminate your stream, enter the following into your terminal: \
ps -ef | grep spark |  grep [DriverProgramName] | awk '{print $2}'   | xargs kill  -SIGTERM