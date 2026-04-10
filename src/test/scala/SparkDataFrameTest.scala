package NYC_Taxi_Pipeline_Test

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType}
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll

class SparkDataFrameTest extends FunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("SparkDataFrameTest")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("DataFrame creation and basic operations") {
    val data = Seq(
      ("John", 25, 50000.0),
      ("Jane", 30, 60000.0),
      ("Bob", 35, 70000.0)
    )

    val df = spark.createDataFrame(data).toDF("name", "age", "salary")

    assert(df.count() == 3)
    assert(df.columns.length == 3)
  }

  test("DataFrame filtering") {
    val data = Seq(
      ("NYC", 1),
      ("LA", 2),
      ("NYC", 3)
    )

    val df = spark.createDataFrame(data).toDF("city", "value")
    val filtered = df.filter(col("city") === "NYC")

    assert(filtered.count() == 2)
  }

  test("DataFrame aggregation") {
    val data = Seq(
      ("A", 10),
      ("B", 20),
      ("A", 15)
    )

    val df = spark.createDataFrame(data).toDF("category", "amount")
    val agg = df.groupBy("category").agg(sum("amount").as("total"))

    assert(agg.count() == 2)

    val result = agg.collect()
    val categoryA = result.find(row => row.getAs[String]("category") == "A")
    assert(categoryA.isDefined)
    assert(categoryA.get.getAs[Long]("total") == 25)
  }

  test("DataFrame join operation") {
    val data1 = Seq((1, "A"), (2, "B")).toDF("id", "name")
    val data2 = Seq((1, 100), (2, 200)).toDF("id", "value")

    val joined = data1.join(data2, "id")

    assert(joined.count() == 2)
    assert(joined.columns.length == 3)
  }

}
