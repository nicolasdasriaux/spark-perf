import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CoalesceRepartitionSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Coalesce Repartition")
    .master("local[*]")
    .config("spark.default.parallelism", 8) // Default parallelism in Spark
    .config("spark.sql.shuffle.partitions", 200) // Parallelism when shuffling in Spark SQL

    .config("spark.sql.join.preferSortMergeJoin", true)
    .config("spark.sql.autoBroadcastJoinThreshold", -1)

    .enableHiveSupport()
    .getOrCreate()

  override def afterAll() {
    SparkPerf.keepSparkUIAlive()
    sparkSession.stop()
  }

  "Absence of coalesce and repartition" should "let Spark SQL do shuffling with default parallelization (200)" in {
    implicit val spark: SparkSession = sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val ordersDS = ECommerce.ordersDS(10, customerId => (customerId + 1) * 1000)

    val orderCountsDF = ordersDS
      .groupBy($"customer_id")
      .agg(count($"id").as("order_count"))

    orderCountsDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("order_counts")
  }

  "Presence of coalesce" should "not add shuffling stage but impact parallelization of Spark SQL shuffling (20 instead of 200)" in {
    implicit val spark: SparkSession = sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val ordersDS = ECommerce.ordersDS(10, customerId => (customerId + 1) * 1000)

    val orderCountsDF = ordersDS
      .groupBy($"customer_id")
      .agg(count($"id").as("order_count"))
      .coalesce(20)

    orderCountsDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("order_counts_coalesce")
  }

  "Presence of repartition" should "not add shuffling stage (20) but not impact parallelization of Spark SQL shuffling (200)" in {
    implicit val spark: SparkSession = sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val ordersDS = ECommerce.ordersDS(10, customerId => (customerId + 1) * 1000)

    val orderCountsDF = ordersDS
      .groupBy($"customer_id")
      .agg(count($"id").as("order_count"))
      .repartition(20)

    orderCountsDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("order_counts_repartition")
  }
}
