# TWITTER DATA ANALYSIS #
## Project Description ##
A Scala project that utilizes Spark and its technologies, such as RDDs, Spark SQL, and Spark Streaming to grab, format, and analyze data pertaining to tweets on Twitter.

## Technologies Used ##
- Scala - version 2.12.12
- Spark - version 3.0.1
- AWS S3 

## Features ##
### Current: ###
- [twitter-stream](https://github.com/revature-scalawags/scalawags-group-5/tree/master/twitter-stream) - Spark application that utilizes Sparks Streaming to pull in real-time twitter data, then filter it out based on hashtags and a count of those hashtags.
- [twitter-stream-analysis](https://github.com/revature-scalawags/scalawags-group-5/tree/master/twitter-stream-analysis) - Spark application that analyzes data generated from streaming Tweets in real time, finding highest counts of hashtags for each letter of the alphabet
- [batch-data-init](https://github.com/revature-scalawags/scalawags-group-5/tree/master/batch-data-init) - Takes a large file containing [old twitter data](https://archive.org/search.php?query=collection%3Atwitterstream&sort=-publicdate) and formats the data to have a structured format. 
- [spark-submit](https://github.com/revature-scalawags/scalawags-group-5/tree/master/spark-submit) - Spark application that analyzes data formatted from batch-data-init. This analysis takes in data so that sparkSQL can query the data to grab specific parts of the data. Then a storage of values and calculation is then placed on the data. Finally, the calculations are then stored on disk.

### To-Do: ###
- Implement Spark Structured Streaming 

## Getting Started ##
- Clone this repository with the following command:
```bash
git clone https://github.com/revature-scalawags/scalawags-group-5.git
```
- Follow the relevant instructions for your specific application within this project: 

- [twitter-stream](https://github.com/revature-scalawags/scalawags-group-5/tree/master/twitter-stream)
- [twitter-stream-analysis](https://github.com/revature-scalawags/scalawags-group-5/tree/master/twitter-stream-analysis)
- [batch-data-init](https://github.com/revature-scalawags/scalawags-group-5/tree/master/batch-data-init)
- [spark-submit](https://github.com/revature-scalawags/scalawags-group-5/tree/master/spark-submit)


## Contributors ##

Cody Piazza \
Collin Breeding \
Trevor Spear

