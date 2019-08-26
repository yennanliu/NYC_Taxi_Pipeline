brew services start zookeeper
brew services start kafka
brew services restart zookeeper 
brew services restart kafka
echo “this is just a test” | kafka-console-producer --broker-list localhost:9092 --topic new_topic
kafka-console-consumer  --bootstrap-server 127.0.0.1:9092 --topic new_topic 
