docker exec spark-master bash -c "mkdir datalake"
docker exec spark-master bash -c "mkdir datawarehouse"
docker cp people.json spark-master:datalake/people.json

echo "build.sbt must have:"
echo '   scalaVersion := "2.12.10"'
echo '   libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.1"'