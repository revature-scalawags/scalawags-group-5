sbt package
docker cp target/scala-2.12/spark-submit_2.12-1.0.jar spark-master:/tmp/spark-submit_2.12-1.0.jar
docker exec spark-master bash -c "./spark/bin/spark-submit --class 'Main' --master local[4] /tmp/spark-submit_2.12-1.0.jar"