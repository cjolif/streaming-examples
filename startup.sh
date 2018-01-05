#!/bin/sh
./kafka_2.11-1.0.0/bin/zookeeper-server-start.sh ./kafka_2.11-1.0.0/config/zookeeper.properties > zookeeper.log&
sleep 10
./kafka_2.11-1.0.0/bin/kafka-server-start.sh ./kafka_2.11-1.0.0/config/server.properties > kafka.log&
sleep 10
./kafka_2.11-1.0.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic origin
./kafka_2.11-1.0.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic flink-destination
./kafka_2.11-1.0.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic kafka-destination
./kafka_2.11-1.0.0/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic spark-destination

#./bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic flink-destination --from-beginning --property print.key=true --property key.separator=:
#./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic origin --property parse.key=true --property key.separator=:





