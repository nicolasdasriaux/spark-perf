import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Sort Merge Join
  *
  * (1) Read '''Sort Merge Join''' section of the following page
  *     [[https://medium.com/@achilleus/https-medium-com-joins-in-apache-spark-part-3-1d40c1e51e1c]]
  *
  * (2) Optionally read the following page
  *     [[https://www.waitingforcode.com/apache-spark-sql/sort-merge-join-spark-sql/read]]
  *
  * (3) Run the test class.
  *     Eventually it will block in [[SortMergeJoinSpec.afterAll]] on [[SparkPerf.keepSparkUIAlive()]] keeping Spark UI alive.
  *
  * (4) Open Spark UI in browser [[http://localhost:4040]]
  *
  * (5) Remember estimated Size per Row (bytes)
  *
  * Customer: 36 = 8 + 8 (LongType) + 20 (StringType)
  * Order: 24 = 8 + 8 (LongType) + 8 (LongType)
  *
  * (6) Follow instructions for each of the test cases
  */

class SortMergeJoinSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Sort Merge Join")
    .master("local[*]")
    .config("spark.default.parallelism", 8) // Default parallelism in Spark
    .config("spark.sql.shuffle.partitions", 200) // Parallelism when shuffling in Spark SQL

    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.sql.join.preferSortMergeJoin", false)
    .getOrCreate()

  override def afterAll() {
    SparkPerf.keepSparkUIAlive()
    sparkSession.stop()
  }

  "Sort Merge Join" should "be performed when conditions apply" in {
    /**
      * Observing Physical Plan with `BroadcastHashJoin`
      *
      * (7) Observe plan for query in '''Spark UI'''
      *     - Notice `SortMergeJoin` node
      *     - Notice `Sort` nodes
      *     - Notice `Exchange` nodes (shuffle)
      */

    /**
      * Understanding Applicability of '''Sort Merge Join'''
      *
      * (8) Look for cases that output `SortMergeJoinExec`
      *     [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.apply()]]
      *
      * See how '''Broadcast Hash Join''' and then '''Shuffled Hash Join''' are tested for applicability
      * before attempting with '''Sort Merge Join'''
      *
      * (8) Look at `spark.sql.join.preferSortMergeJoin` config
      *     [[org.apache.spark.sql.internal.SQLConf.PREFER_SORTMERGEJOIN]]
      *     [[org.apache.spark.sql.internal.SQLConf.preferSortMergeJoin]]
      *
      * When true, prefer sort merge join over shuffle hash join.
      *
      * See how to disable '''Shuffled Hash Join''' (unless '''Sort Merge Join''' is not applicable)
      *
      * (9) Look at `spark.sql.autoBroadcastJoinThreshold` config
      *     [[org.apache.spark.sql.internal.SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]]
      *     [[org.apache.spark.sql.internal.SQLConf.autoBroadcastJoinThreshold]]
      *
      * Configures the maximum size in bytes for a table that will be broadcast to all worker
      * nodes when performing a join. By setting this value to -1 broadcasting can be disabled.
      *
      * See how to disable '''Broadcast Hash Join''' (by sizes)
      */

    implicit val spark: SparkSession = sparkSession
    import spark.implicits._

    val customersDS = ECommerce.customersWithKnownRowCountDS(4) //
    val ordersDS = ECommerce.ordersWithKnownRowCountDS(4, customerId => 100)

    val customersAndOrdersDF = customersDS.as("cst")
      .join(ordersDS.as("ord"), $"cst.id" === $"ord.customer_id")
      .select($"cst.id".as("customer_id"), $"cst.name", $"ord.id".as("order_id"))

    customersAndOrdersDF.collect()
    customersAndOrdersDF.queryExecution.toString().contains("SortMergeJoin") should be(true)
  }
}
