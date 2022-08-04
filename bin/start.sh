#!/usr/bin/env bash

source /etc/profile
#jps
#echo $1,$2,$3
#start-dfs.sh
#ssh $2 "start-yarn.sh"
#zkServer.sh start
#ssh $2 "zkServer.sh start"
#ssh $3 "zkServer.sh start"
#kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
#ssh $2 "kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties"
#ssh $3 "kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties"
#kafka-topics.sh --create --bootstrap-server $1:9092,$2:9092,$3:9092 --topic test --replication-factor 3 --partitions 1
#kafka-topics.sh --create --bootstrap-server hadoop103:9092,hadoop104:9092,hadoop105:9092 --topic test --replication-factor 3 --partitions 1
#mkdir /root/log
#touch /root/log/log.log
#ssh $2 "mkdir /root/log"
#ssh $2 "touch /root/log/log.log"
#ssh $3 "mkdir /root/log"
#ssh $3 "touch /root/log/log.log"

a=$(find / -name "FilmRecommendationSystem")
pushd $a
#scp ./src/flume/arvo_kafka.conf root@$3:/root/
#scp ./src/flume/kafka_tcp.conf root@$3:/root/
#ssh $3 "nohup flume-ng agent --name arvo_kafka --conf conf -f /root/arvo_kafka.conf >> /root/log/log.log &"
#nohup flume-ng agent --name log_arvo --conf conf -f ./src/flume/log_arvo.conf >> /root/log/log.log &
#ssh $3 "nohup flume-ng agent --name kafka_tcp --conf conf -f /root/kafka_tcp.conf >> /root/log/log.log &"

cd packages/
#pwd
rm -rf ../packages.zip
zip -r ../packages.zip .
cd ..
#pwd
spark-submit --master yarn --py-files packages.zip test1.py
