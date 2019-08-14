import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Bucketing
  *
  * (1) Read '''Bucketing''' and '''Bucketing Tips''' sections of the following page
  *     [[https://mapr.com/blog/tips-and-best-practices-to-take-advantage-of-spark-2-x/]]
  *
  * (2) Optionally read the following page especially the '''Bucket definition''' section
  *     [[https://www.waitingforcode.com/apache-spark-sql/apache-spark-2.4.0-features-bucket-pruning/read]]
  *
  * (3) Run the test class.
  *     Eventually it will block in [[BucketingSpec.afterAll]] on [[SparkPerf.keepSparkUIAlive()]] keeping Spark UI alive.
  *
  * (4) Open Spark UI in browser [[http://localhost:4040]]
  *
  * (5) Follow instructions for each of the test cases
  */

class BucketingSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Bucketing")
    .master("local[*]")
    .config("spark.default.parallelism", 8) // Default parallelism in Spark
    .config("spark.sql.shuffle.partitions", 200) // Parallelism when shuffling in Spark SQL

    .enableHiveSupport()
    .getOrCreate()

  override def afterAll() {
    SparkPerf.keepSparkUIAlive()
    sparkSession.stop()
  }

  "Absence of Bucketing" should "not allow to avoid shuffling" in {
    /**
      * Observing Physical Plan when absence of bucketing
      *
      * (6) Observe plan for query in '''Spark UI'''
      *     - Presence of `Scan` fully reading the table
      *     - Presence of `Exchange` node (shuffling) between 2 `HashAggregate` nodes
      *     - `partial_count` for the 1st `HashAggregate` node
      *     - `count` for the 2nd `HashAggregate` node
      */

    implicit val spark: SparkSession = sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val ordersDS = ECommerce.ordersDS(100, customerId => 1000)

    ordersDS.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("orders_no_bucket")

    val orderCountsDF = spark.table("orders_no_bucket")
      .where($"customer_id".isin(1 to 10: _*))
      .groupBy($"customer_id")
      .agg(count($"id").as("order_count"))

    orderCountsDF.collect()
    orderCountsDF.queryExecution.toString() shouldNot include regex """SelectedBucketsCount: \d+ out of 10"""
  }

  "Bucketing" should "allow to avoid shuffling and to target buckets" in {
    /**
      * Observing Storage Structure of bucketed table
      *
      * (7) Study the code below that does '''Bucketed Table Creation'''
      *
      * (8) Observe structure of saved `orders_bucket` table
      *     - Look at `spark-warehouse/orders_bucket` folder
      *
      *     - Observe `part-PPPPP-xxxxxxxxxxxxxxxx_BBBBB.xxxx.snappy.parquet` files inside
      *       - PPPPP is a part number
      *       - BBBBB is a bucket number (0 to 9, there are 10 buckets)
      *       - So every part is divided into multiple buckets
      *       - Each row goes to a bucket that is calculated
      *         hashing a set of columns (`customer_id`) and applying modulo (10)
      */

    /**
      * Observing Physical Plan when presence of bucketing
      *
      * (9) Observe plan for query in '''Spark UI'''
      *     - Presence of `Scan` node reading only potential buckets
      *     - Presence of `Filter` node to select from potential buckets
      *     - Absence of `Exchange` node (shuffling) (useless thanks to bucket '''pre-hash''')
      *     - `partial_count` for the 1st `HashAggregate` node
      *     - `count` for the 2nd `HashAggregate` node
      */

    implicit val spark: SparkSession = sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val ordersDS = ECommerce.ordersDS(100, customerId => 1000)

    ordersDS.write
      .mode(SaveMode.Overwrite)
      .bucketBy(10, "customer_id")
      .sortBy("id")
      .saveAsTable("orders_bucket")

    // Bucketed Table Creation
    val orderCountsDF = spark.table("orders_bucket")
      .where($"customer_id".isin(1 to 10: _*))
      .groupBy($"customer_id")
      .agg(count($"id").as("order_count"))

    orderCountsDF.collect()
    orderCountsDF.queryExecution.toString() should include regex """SelectedBucketsCount: \d+ out of 10"""
  }
}
