name := "flink"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += Resolver.sonatypeRepo("snapshots")

libraryDependencies ++= Seq(

  "org.apache.flink" %% "flink-streaming-java" % "1.10.0",
  "org.apache.flink" %% "flink-streaming-scala" % "1.10.0",
  "org.apache.flink" %% "flink-runtime-web" % "1.10.0",
  "org.elasticsearch" % "elasticsearch" % "7.6.1",
  "joda-time" % "joda-time" % "2.10.5"

)

//conflictManager := ConflictManager.latestRevision

//mainClass := Some("ETLdev.CollectValueZonesEMR")
//mainClass := Some("sparkhelloworld.SparkProcessGameRDD")

// assemblyMergeStrategy in assembly := {
//   case PathList("META-INF", xs @ _*) => MergeStrategy.discard
//   case x => MergeStrategy.first
// }