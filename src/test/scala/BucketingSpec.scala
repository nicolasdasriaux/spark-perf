import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

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
    implicit val spark: SparkSession = sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val ordersDS = ECommerce.ordersDS(100, customerId => 1000)

    ordersDS.write
      .mode(SaveMode.Overwrite)
      .bucketBy(10, "customer_id")
      .sortBy("id")
      .saveAsTable("orders_bucket")

    val orderCountsDF = spark.table("orders_bucket")
      .where($"customer_id".isin(1 to 10: _*))
      .groupBy($"customer_id")
      .agg(count($"id").as("order_count"))

    orderCountsDF.collect()
    orderCountsDF.queryExecution.toString() should include regex """SelectedBucketsCount: \d+ out of 10"""
  }
}
