name := "NYC_Taxi_Pipeline"

version := "1.0"

scalaVersion := "2.11.8"

val sparkVersion = "2.3.0"

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(
  // config
  "com.typesafe" % "config" % "1.2.1", 
  // spark  
  "org.apache.spark" %% "spark-core" % "2.3.0",
  "org.scalactic" %% "scalactic" % "3.0.8",
  "org.scalatest" %% "scalatest" % "3.0.8" % "test",
  "org.apache.spark" %% "spark-sql" % "2.3.0",
  "org.apache.spark" %% "spark-mllib" % "2.2.0",
  "com.databricks" %% "spark-csv" % "1.4.0",
  "com.amazonaws" % "aws-java-sdk" % "1.7.4",
  "org.apache.hadoop" % "hadoop-aws" % "2.7.6"

  // spark stream 
  // "org.apache.spark" %% "spark-streaming" % "2.3.1",
  // "org.apache.spark" % "spark-sql-kafka-0-10_2.11" % "2.2.1",
  // "org.apache.spark" % "spark-streaming-kafka-0-8_2.11" % "2.2.1",
  // "org.apache.spark" %% "spark-streaming-kinesis-asl" % "2.2.1",
  // "org.apache.bahir" %% "spark-streaming-twitter" % "2.3.1",
  // "com.github.catalystcode" %% "streaming-reddit" % "0.0.1",
  // "com.danielasfregola" %% "twitter4s" % "6.1"
)

conflictManager := ConflictManager.latestRevision

//mainClass := Some("ETLdev.CollectValueZonesEMR")
//mainClass := Some("sparkhelloworld.SparkProcessGameRDD")

// assemblyMergeStrategy in assembly := {
//   case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//   case x => MergeStrategy.first
// }