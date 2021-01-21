docker exec spark-master bash -c "mkdir datalake"
docker exec spark-master bash -c "mkdir datalake/month"
docker exec spark-master bash -c "mkdir datawarehouse"
sbt "run -y 2020 -m 03 -d 01 -min 00"
docker cp 2020_03_01_00.json spark-master:datalake/month/2020_03_01_00.json
