package utils

import com.typesafe.config.ConfigFactory

object GetCreds {

  // mongo
  val mongodbconfig = ConfigFactory.load().getConfig("mongodb")

  val connection_string: String = mongodbconfig.getString("CONNECTION_STRING")
  val database_name: String = mongodbconfig.getString("DATABASE")
  val collection_name: String = mongodbconfig.getString("COLLECTION")

  // kafka
  val kafkaconfig = ConfigFactory.load().getConfig("kafka")
  val bootstrap_servers: String = kafkaconfig.getString("BOOTSTRAP_SERVERS")
  val topic: String = kafkaconfig.getString("TOPIC")

  // spark
  val sparkconfig = ConfigFactory.load().getConfig("spark")
  val master_url: String = sparkconfig.getString("MASTER_URL")

  println(connection_string)
  println(database_name)
  println(collection_name)

  println(bootstrap_servers)
  println(topic)

  println(master_url)

}