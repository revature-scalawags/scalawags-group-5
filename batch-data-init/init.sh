docker exec spark-master bash -c "mkdir datalake"
docker exec spark-master bash -c "mkdir datalake/month"
docker exec spark-master bash -c "mkdir datalake/hour"
docker exec spark-master bash -c "mkdir datawarehouse"
docker exec spark-master bash -c "mkdir datawarehouse/month"
docker exec spark-master bash -c "mkdir datawarehouse/hour"

# The entire day
sbt "run -y 2020 -m 03 -d 01"
docker cp 2020_03_01.json spark-master:datalake/month/2020_03_01.json

# Only one hour
sbt "run -y 2020 -m 03 -d 01 -h 00"
docker cp 2020_03_01_00.json spark-master:datalake/hour/2020_03_01_00.json
