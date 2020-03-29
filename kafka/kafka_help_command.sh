#!/bin/sh

# start zookeeper, kafka
brew services start zookeeper
brew services start kafka

# restart zookeeper, kafka
brew services restart zookeeper 
brew services restart kafka

# stop zookeeper, kafka
brew services stop zookeeper
brew services stop kafka

# Clean kafka dara 
rm /Users/$USER/kafka_data/kafka/*

# Config Kafka and Zoopkeeper
nano /usr/local/etc/kafka/server.properties
nano /usr/local/etc/kafka/zookeeper.properties

# Delete Kafka topic 
# modify /usr/local/etc/kafka/server.properties 
# and make delete.topic.enable=true
kafka-topics  --delete --zookeeper localhost:2181 --topic <your_topic_name>

# Creat a new topic 
kafka-topics --create -zookeeper localhost:2181 --replication-factor 1  --partitions 1 --topic streams-taxi

# list topics 
kafka-topics  --zookeeper  127.0.0.1:2181 --list 

# set up producer  
kafka-console-producer  --broker-list  127.0.0.1:9092 --topic first_topic --producer-property acks=all  

# set up cosumer (read from now, i.e. only read the producer msg when this consumer is set up ) (NOW, OPEN THE OTHER TERMIANL AND RUN THE BELOW COMMAND )
kafka-console-consumer   --bootstrap-server  127.0.0.1:9092 --topic first_topic 

# set up cosumer (read from beginning, i.e. read all msg) (NOW, OPEN THE OTHER TERMIANL AND RUN THE BELOW COMMAND )
kafka-console-consumer   --bootstrap-server  127.0.0.1:9092 --topic first_topic  --from-beginning 

# curl event to kafka producer
curl localhost:44444 | kafka-console-producer  --broker-list  127.0.0.1:9092 --topic first_topic
