# spark-submit

## Project Description
Spark application that analyzes data formatted from batch-data-init. This analysis takes in data so that sparkSQL can query the data to grab specific parts of the data. Then a storage of values and calculation is then placed on the data. Finally, the calculations are then stored on disk.

## Technologies Used

* spark - version 3.0.1
* hadoop - version 3.2
* scala - version 2.12.10
* sbt - version 1.4.4

## Features
- Utilizes Datasets for sparkSQL to be able to query
- Does 5 queries:
    - Text - Removes special characters and foreign language and counts the words.
    - Source - Counts all sources of tweets: IOS, Android, and Web 
    - Hashtag - Counts up all hashtags in tweets.
    - Verified - Counts all the tweets if they are verified or not.
    - Language - Counts all the different languages twitter detects in the tweet.
- Outputs highest counts of each query
- Generates readable console output as well as a csv file for each query


## Getting Started
Follow all the steps in [batch-data-init Usage](https://github.com/revature-scalawags/scalawags-group-5/tree/master/batch-data-init#run) before getting started.
This is where to find the data and format it correct for this application.


## Usage
If batch-data-init was followed and example was used then simply:

    sh run.sh

Otherwise:
1. sbt package # Makes the jar file for spark to run.

2. docker cp target/scala-2.12/spark-submit_2.12-1.0.jar spark-master:/tmp/spark-submit_2.12-1.0.jar

The only difference in the 3rd step is folder input. Make sure to point the application to the correct folder. The FILE isn't needed unless you want the spark application to use only that file in the analysis.

3. docker exec spark-master bash -c "./spark/bin/spark-submit --class 'Main' --driver-memory 12g --master local[*] /tmp/spark-submit_2.12-1.0.jar FOLDER FILE"

