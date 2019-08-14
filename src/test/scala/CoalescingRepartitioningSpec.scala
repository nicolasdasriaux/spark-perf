import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Coalescing and Repartitioning
  *
  * (1) Run the test class.
  *     Eventually it will block in [[CoalescingRepartitioningSpec.afterAll]] on [[SparkPerf.keepSparkUIAlive()]] keeping Spark UI alive.
  *
  * (2) Open Spark UI in browser [[http://localhost:4040]]
  *
  * (3) Follow instructions for each of the test cases
  */

class CoalescingRepartitioningSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Coalescing Repartitioning")
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

  "Absence of coalesce and repartition" should "let Spark SQL do shuffling with defined parallelism (200)" in {
    /**
      * Observing Physical Plan when absence of coalescing and repartitioning
      *
      * (4) Observe plan for query in '''Spark UI'''
      *     - Inspect Query 0
      *     - Inspect related Job 0
      *     - Notice Stage 0 and Stage 1 and associated number of tasks
      *     - Notice how number of tasks for Stage 0 relates to `spark.default.parallelism`
      *     - Notice shuffling (`Exchange` node) between Stage 0 and Stage 1
      *     - Notice how number of tasks for Stage 1 (after shuffling) relates to `spark.sql.shuffle.partitions`
      *       [[org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS]]
      *
      * (5) Observe parts of saved `order_counts` table
      *     - Look at `spark-warehouse/order_counts` folder
      *     - Observe `part-PPPPP-xxxxxxxxxxxxxxxx.snappy.parquet` files
      *     - Notice how they are numbered by task (PPPPP) and how each task wrote its corresponding part
      *     - Notice how some tasks did not write their part because of absence of data on the partition
      *     - Be aware there could have been as many file as tasks in Stage 1 (200)
      *     - Be aware there are many small files and it might be suboptimal
      */

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

  "Presence of coalesce" should "not add shuffling but impact parallelism of previous stage (20 instead of 200)" in {
    /**
      * Observing Physical Plan when coalescing
      *
      * (6) Observe plan for query in '''Spark UI'''
      *     - Inspect Query 1
      *     - Inspect related Job 1
      *     - Notice Stage 2 and Stage 3 and associated number of tasks
      *     - Notice how number of tasks for Stage 2 still relates to `spark.default.parallelism`
      *     - Notice shuffling (`Exchange` node) between Stage 2 and Stage 3 just as before
      *     - Notice how number of tasks for Stage 3 relates to parameter of `coalesce` (and not to `spark.sql.shuffle.partitions` anymore)
      *     - Notice how `coalesce` does NOT add shuffling (no additional `Exchange` node)
      *     - Become aware that `coalesce` avoids shuffling BUT changes parallelism of previous stage (potentially reducing it)
      *
      * (7) Observe parts of saved `order_counts_coalesce` table
      *     - Look at `spark-warehouse/order_counts_coalesce` directory
      *     - Observe `part-PPPPP-xxxxxxxxxxxxxxxx.snappy.parquet` files
      *     - Notice how they are numbered by task (PPPPP) and how each task wrote its corresponding part
      *     - Notice how some tasks did not write their part because of absence of data on the partition
      *     - Be aware there could have been as many files as tasks in Stage 3 (20)
      *     - Be aware there are now less but bigger parts, at the cost of less parallelism (but no shuffling)
      */

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

  "Presence of repartition" should "not add shuffling (20) but does not impact parallelism of previous stage (200)" in {
    /**
      * Observing Physical Plan when coalescing
      *
      * (8) Observe plan for query in '''Spark UI'''
      *     - Inspect Query 2
      *     - Inspect related Job 2
      *     - Notice Stage 4, Stage 5 and Stage 6 and associated number of tasks
      *     - Notice how number of tasks for Stage 4 still relates to `spark.default.parallelism`
      *     - Notice shuffling (`Exchange` node) between Stage 4 and Stage 5 just as before
      *     - Notice how number of tasks for Stage 5 relates to `spark.sql.shuffle.partitions` (and not to parameter of `repartition`)
      *     - Notice how `repartition` adds shuffling (additional `Exchange` node) between Stage 5 and an additional Stage 6
      *     - Notice how number of tasks for Stage 6 relates to parameter of `repartition` (and not to `spark.sql.shuffle.partitions` anymore)
      *     - Become aware that `repartition` leaves parallelism of previous stage unchanged BUT adds shuffling (and an additional stage)
      *
      * (9) Observe parts of saved `order_counts_repartition` table
      *     - Look at `spark-warehouse/order_counts_repartition` directory
      *     - Observe `part-PPPPP-xxxxxxxxxxxxxxxx.snappy.parquet` files inside
      *     - Notice how they are numbered by task (PPPPP) and how each task wrote its corresponding part
      *     - Notice how some tasks did not write their part because of absence of data on the partition
      *     - Be aware there could have been as many files as tasks in Stage 6 (20)
      *     - Be aware there are now less but bigger parts, at the cost of shuffling (but preserved parallelism)
      */

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
