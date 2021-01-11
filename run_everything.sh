# Makes a csv with all the twitter-data
cd twitter-data
sbt run
cd ..

# Makes the jar
cd spark-submit
sbt compile
sbt assemble
cd ..

# Moves the csv and jar into docker container
docker cp 
