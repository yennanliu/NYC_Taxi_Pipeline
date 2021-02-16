package ETLdev

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CollectValueZonesEMR {
  private val logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]) {

    // check arguments
    // if (args.length != 4)
    //   throw new IllegalArgumentException(
    //     "Parameters : "+
    //     "<yellowSource> <greenSource> <zonesSource> <targetBucket> "+
    //     "(multiple source paths can be provided in the same string, separated by a coma"
    //   )

    logger.setLevel(Level.INFO)
    lazy val session =
      SparkSession.builder
        .appName("nyctaxi-value-zones")
        .getOrCreate()

    try {
      // runJob(sparkSession = session,
      //       yellow = args(0).split(",").toList,
      //       green = args(1).split(",").toList,
      //       zones = args(2).split(",").toList,
      //       target = args(3)
      //       )
      runJob(sparkSession = session)
      session.stop()
    } catch {
      case ex: Exception =>
        logger.error(ex.getMessage)
        logger.error(ex.getStackTrace.toString)
    }
  }

  def runJob(sparkSession: SparkSession) = {

    logger.info("Execution started")

    import sparkSession.implicits._

    sparkSession.conf.set("spark.sql.session.timeZone", "America/New_York")

    print(">>>>>>>>>>LOAD S3 yellowEvents")
    val yellowEvents = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("enforceSchema", "false")
      .option("timeStampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("columnNameOfCorruptRecord", "error")
      .csv("s3a://nyc-tlc-taxi/trip data/yellow_tripdata_2019-([0-9]*).csv") //.csv(yellow: _*)
      .filter(col("tpep_pickup_datetime").gt("2017"))
      .filter(col("tpep_pickup_datetime").lt("2019"))
      .withColumn("duration", unix_timestamp($"tpep_dropoff_datetime").minus(unix_timestamp($"tpep_pickup_datetime")))
      .withColumn("minute_rate", $"total_amount".divide($"duration") * 60)
      .withColumnRenamed("tpep_pickup_datetime", "pickup_datetime")
      .select("pickup_datetime", "minute_rate", "PULocationID", "total_amount")
      .withColumn("taxiColor", lit("yellow"))

    print(">>>>>>>>>>LOAD S3 greenEvents")
    val greenEvents = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("enforceSchema", "false")
      .option("timeStampFormat", "yyyy-MM-dd HH:mm:ss")
      .option("columnNameOfCorruptRecord", "error")
      .csv("s3a://nyc-tlc-taxi/trip data/green_tripdata_2019-([0-9]*).csv") //.csv(green: _*)
      .filter(col("lpep_pickup_datetime").gt("2017"))
      .filter(col("lpep_pickup_datetime").lt("2019")).withColumn("duration", unix_timestamp($"lpep_dropoff_datetime").minus(unix_timestamp($"lpep_pickup_datetime")))
      .withColumn("minute_rate", $"total_amount".divide($"duration") * 60)
      .withColumnRenamed("lpep_pickup_datetime", "pickup_datetime")
      .select("pickup_datetime", "minute_rate", "PULocationID", "total_amount")
      .withColumn("taxiColor", lit("green"))

    print(">>>>>>>>>>LOAD S3 zonesInfo")
    val zonesInfo = sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .option("enforceSchema", "false")
      .option("columnNameOfCorruptRecord", "error")
      .csv("s3a://nyc-tlc-taxi/zone/taxi+_zone_lookup.csv") //.csv(zones: _*)

    val allEventsWithZone = greenEvents
      .union(yellowEvents)
      .join(zonesInfo, $"PULocationID" === $"LocationID")
      .select("pickup_datetime", "minute_rate", "taxiColor", "LocationID", "Borough", "Zone")

    allEventsWithZone.cache

    val zoneAttractiveness = allEventsWithZone
      .groupBy($"LocationID", date_trunc("hour", $"pickup_datetime") as "pickup_hour")
      .pivot("taxiColor", Seq("yellow", "green"))
      .agg("minute_rate" -> "avg", "minute_rate" -> "count")
      .withColumnRenamed("yellow_avg(minute_rate)", "yellow_avg_minute_rate")
      .withColumnRenamed("yellow_count(minute_rate)", "yellow_count")
      .withColumnRenamed("green_avg(minute_rate)", "green_avg_minute_rate")
      .withColumnRenamed("green_count(minute_rate)", "green_count")

    print(">>>>>>>>>> SAVE OUTPUT TO S3 (parquet)")
    val rawQuery = allEventsWithZone
      .withColumn("year", year($"pickup_datetime"))
      .withColumn("month", month($"pickup_datetime"))
      .withColumn("day", dayofmonth($"pickup_datetime"))
      .repartition($"year", $"month")
      .sortWithinPartitions("day")
      .write
      .mode("OVERWRITE")
      .partitionBy("year", "month")
      .parquet("s3a://nyc-tlc-taxi/etl_output/raw-rides/rawQuery")

    val aggregateQuery = zoneAttractiveness
      .repartition(1)
      .sortWithinPartitions($"pickup_hour")
      .write
      .mode("OVERWRITE")
      .parquet("s3a://nyc-tlc-taxi/etl_output/raw-rides/aggregateQuery")
  }

}