import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Estimating Size per Row
  *
  * Catalyst optimizer needs to estimate Size per Row to optimize queries using dataframes and datasets.
  */

/**
  * Estimating Size per Columns (in bytes)
  *
  * (1) Look at `defaultSize` method
  *     [[org.apache.spark.sql.types.DataType.defaultSize]]
  *
  * (2) Look at `defaultSize` method for various data types
  *     [[org.apache.spark.sql.types.LongType.defaultSize]]
  *     [[org.apache.spark.sql.types.StringType.defaultSize]]
  */

/**
  * Estimating Size per Row (in bytes)
  *
  * (3) Look at how size for a row is estimated
  *     [[org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils.getSizePerRow()]]
  *
  * (4) It should be 8 + sum of sizes of all columns
  */

/**
  * Estimating size for Customer and Order
  *
  * (5) Manually estimate size per row for a `Dataset` of [[Customer]]
  * (6) Manually estimate size per row for a `Dataset` of [[Order]]
  */

class EstimatingSizePerRowSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Broadcast Hash Join")
    .master("local[*]")
    .config("spark.default.parallelism", 8) // Default parallelism in Spark
    .config("spark.sql.shuffle.partitions", 200) // Parallelism when shuffling in Spark SQL
    .getOrCreate()

  override def afterAll() {
    SparkPerf.keepSparkUIAlive()
    sparkSession.stop()
  }
}

/**
  * HINTS
  */

/**
  * Estimated Size per Row (bytes)
  *
  * `Customer`: 36 = 8 + 8 (`LongType`) + 20 (`StringType`)
  * `Order`: 24 = 8 + 8 (`LongType`) + 8 (`LongType`)
  */
