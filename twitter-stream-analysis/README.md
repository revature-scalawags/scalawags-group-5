# TWITTER STREAM ANALYSIS #

## Project Description ##
Spark application that analyzes data generated from streaming Tweets in real time, finding highest counts of hashtags for each letter of the alphabet 

## Technologies Used ##
- Scala - version 2.12.12
- Spark - version 3.0.1
- Spark RDDs
- awscala-s3 package

## Features ##
- Utilizes RDDs to filter through generated hashtag data
- Outputs highest counts of hashtags for each letter of the alphabet
- Generates readable console output as well as a csv file
- Saves output csv file to an S3 bucket on the cloud

## Usage ##
- Navigate to the "twitter-stream-analysis" directory in your shell, then run "sbt assembly"
- Once the jar is generated, it will be located in target/scala-x.xx. Copy this jar over to your spark cluster.
- Save your AWS S3 credentials as environment variables in your spark cluster shell session in the following format:
```
AWS_ACCESS_KEY_ID=*******************
AWS_SECRET_ACCESS_KEY=**********************************
```
- Use the "twitter-stream" in this repository program to gather your streamed hashtag data
- Run spark-submit on the jar file, specifying "Main" as the class and specifying the path to your S3 bucket data INPUT DIRECTORY (where your stream is currently outputting data to) as the argument. MAKE SURE THIS PATH IS VALID! For example:
```bash
spark-submit  
--class Main 
--master local[*] 
yourJar.jar 
bucket-name/directory/input-directory 
```
- Your results will be displayed on screen, as well as saved in a .csv format on your S3 bucket