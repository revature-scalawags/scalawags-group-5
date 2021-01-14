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