# Projects
All of the following projects uses spark and data gained from twitter in a different way. More information on how each project works in the README.md in each project.

## spark-submit
Makes a jar to be run on the cluster.

## twitter-data
Uses a twitter api call to grab some temporary batch data. Then does an analysis on the batch data.

## twitter-stream
Streams in data from twitter to the cluster then directly streams it to the AWS S3.

## twitter-stream-analysis
Takes in batch data from previously streamed data from twitter and does an analysis on the data.
