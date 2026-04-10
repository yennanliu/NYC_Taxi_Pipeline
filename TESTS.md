# Spark Tests Documentation

## Running Tests Locally

```bash
sbt test
```

## Test Files

### `SparkBasicTest.scala`
- Tests basic Spark RDD operations
- Verifies data collection, sum, and count operations
- Minimal setup with local SparkContext

### `SparkDataFrameTest.scala`
- Tests DataFrame creation and operations
- Covers:
  - DataFrame creation from Scala sequences
  - Filtering operations
  - Aggregation (groupBy, sum)
  - Join operations
- Uses SparkSession with BeforeAndAfterAll hooks

### `HelloTest.scala`
- Basic Scala test with Map operations
- Tests collection and stack operations

### `TestTaxiRide.scala`
- Domain-specific taxi data tests
- Tests taxi ride-related operations

### `TestNycGeoUtils.scala`
- Tests NYC geographic utilities
- Tests location and area-related functions

## CI/CD Test Execution

### GitHub Actions Workflows

1. **`scala.yml`** - Scala/Spark Build
   - Sets up Java 8 environment
   - Runs `sbt test` to execute all tests
   - Builds assembly JAR
   - Caches sbt dependencies

2. **`spark-docker.yml`** - Docker Build & Spark Tests
   - Builds Docker image with Spark
   - Verifies Spark shell and spark-submit
   - Tests Spark functionality in containerized environment

## Writing New Tests

Template for new Spark tests:

```scala
package NYC_Taxi_Pipeline_Test

import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfterAll

class MySparkTest extends FunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("MySparkTest")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("my test description") {
    // test implementation
  }
}
```

## Test Dependencies

From `build.sbt`:
- `scalatest` 3.1.1 (for testing framework)
- `spark-sql` 2.4.3
- `spark-core` 2.4.3
- `spark-streaming` 2.4.3
