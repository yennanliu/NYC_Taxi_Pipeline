#!/bin/sh

# start zookeeper, kafka
brew services start zookeeper
brew services start kafka

# restart zookeeper, kafka
brew services restart zookeeper 
brew services restart kafka

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