# TWITTER STREAM ANALYSIS #

## Project Description ##
Spark application that analyzes data generated from streaming Tweets in real time, finding highest counts of hashtags for each letter of the alphabet 

## Technologies Used ##
- Scala
- Spark
- Spark RDDs
- awscala-s3 package

## Features ##
- Utilizes RDDs to filter through generated hashtag data
- Outputs highest counts of hashtags for each letter of the alphabet
- Generates readable console output as well as a csv file
- Saves output csv file to an S3 bucket on the cloud

## Usage ##
- Use the "twitter-stream" in this repository program to gather your streamed hashtag data
- Save your AWS credentials as environment variables in your shell
- Run a spark submit on your spark cluster, inputting the following three arguments are the jar file is specified: [InputFilePath] [OutputFileDirectory] [S3BucketOutputPath]
    - Example: 
    <br> ./spark/bin/spark-submit 
    <br>--class Main 
    <br>--master local[*] 
    <br> twitter-stream-analysis.jar
    <br> Results/your-data.txt 
    <br>FinalResults 
    <br>cpiazza01-revature/project2/finalStreamingResults