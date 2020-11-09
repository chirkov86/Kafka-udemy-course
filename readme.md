### Kafka

Kafka API reference: https://kafka.apache.org/documentation/

Starting Kafka locally:
```
cd /kafka_2.13-2.6.0
./bin/zookeeper-server-start.sh config/zookeeper.properties
./bin/kafka-server-start.sh  config/server.properties
./bin/kafka-topics.sh --bootstrap-server localhost:9092 --topic first_topic --create --partitions 3 --replication-factor 1
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic first_topic
```
