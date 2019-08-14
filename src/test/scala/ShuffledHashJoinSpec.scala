import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Shuffled Hash Join
  *
  * (1) Read '''Shuffled Hash Join''' section of the following page
  *     [[https://medium.com/@achilleus/https-medium-com-joins-in-apache-spark-part-3-1d40c1e51e1c]]
  *
  * (2) Optionally read the following page
  *     [[https://www.waitingforcode.com/apache-spark-sql/shuffle-join-spark-sql/read]]
  *
  * (3) Run the test class
  *     Eventually it will block in [[ShuffledHashJoinSpec.afterAll]] on [[SparkPerf.keepSparkUIAlive()]] keeping Spark UI alive.
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

class ShuffledHashJoinSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Shuffled Hash Join")
    .master("local[*]")
    .config("spark.default.parallelism", 8) // Default parallelism in Spark
    .config("spark.sql.shuffle.partitions", 200) // Parallelism when shuffling in Spark SQL

    .config("spark.sql.join.preferSortMergeJoin", false)
    .config("spark.sql.autoBroadcastJoinThreshold", 2)
    .config("spark.sql.shuffle.partitions", 100)
    .getOrCreate()

  override def afterAll() {
    SparkPerf.keepSparkUIAlive()
    sparkSession.stop()
  }

  "Shuffled Hash Join" should "be performed when conditions apply" in {
    /**
      * Observing Physical Plan with `ShuffledHashJoin`
      *
      * (7) Observe plan for query in '''Spark UI''' in '''Spark UI'''
      *     - Notice `ShuffledHashJoin` node
      *     - Notice `Exchange` nodes (shuffle)
      */

    /**
      * Understanding Applicability of '''Shuffled Hash Join'''
      *
      * (8) Look for cases that output `ShuffledHashJoinExec`
      *     [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.apply()]]
      *
      * {{{
      * !conf.preferSortMergeJoin &&
      * canBuildRight(joinType) &&
      * canBuildLocalHashMap(right) &&
      * muchSmaller(right, left) ||
      * !RowOrdering.isOrderable(leftKeys)
      * }}}
      *
      * or
      *
      * {{{
      * !conf.preferSortMergeJoin &&
      * canBuildLeft(joinType) &&
      * canBuildLocalHashMap(left) &&
      * muchSmaller(left, right) ||
      * !RowOrdering.isOrderable(leftKeys)
      * }}}
      *
      * (9) Look at `canBuildLocalHashMap` method
      *     [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.canBuildLocalHashMap]]
      *
      * Matches a plan whose single partition should be small enough to build a hash table.
      * In other words, it's most worthwhile to hash and shuffle data than to broadcast it to all executors.
      *
      * {{{
      * def canBuildLocalHashMap(plan: LogicalPlan): Boolean = {
      *   plan.stats.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions
      * }
      * }}}
      *
      * (10) Look at `muchSmaller` method
      *      [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.muchSmaller]]
      *
      * Returns whether plan a is much smaller (3 times at least) than plan b.
      *
      * {{{
      * def muchSmaller(a: LogicalPlan, b: LogicalPlan): Boolean = {
      *   a.stats.sizeInBytes * 3 <= b.stats.sizeInBytes
      * }
      * }}}
      *
      * (11) Look at `spark.sql.join.preferSortMergeJoin` config
      *      [[org.apache.spark.sql.internal.SQLConf.PREFER_SORTMERGEJOIN]]
      *      [[org.apache.spark.sql.internal.SQLConf.preferSortMergeJoin]]
      *
      * When true, prefer sort merge join over shuffle hash join.
      *
      * (12) Look at `spark.sql.autoBroadcastJoinThreshold` config
      *      [[org.apache.spark.sql.internal.SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]]
      *      [[org.apache.spark.sql.internal.SQLConf.autoBroadcastJoinThreshold]]
      *
      * Configures the maximum size in bytes for a table that will be broadcast to all worker
      * nodes when performing a join. By setting this value to -1 broadcasting can be disabled.
      *
      * (13) Look at `spark.sql.shuffle.partitions` method
      *      [[org.apache.spark.sql.internal.SQLConf.numShufflePartitions]]
      *      [[org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS]]
      *
      * The default number of partitions to use when shuffling data for joins or aggregations.
      */

    implicit val spark: SparkSession = sparkSession
    import spark.implicits._


    val customersDS = ECommerce.customersWithKnownRowCountDS(4) //
    val ordersDS = ECommerce.ordersWithKnownRowCountDS(4, customerId => 100)

    val customersAndOrdersDF = customersDS.as("cst")
      .join(ordersDS.as("ord"), $"cst.id" === $"ord.customer_id")
      .select($"cst.id".as("customer_id"), $"cst.name", $"ord.id".as("order_id"))

    /**
      * Applicability of Shuffled Hash Join
      *
      * {{{
      * !conf.preferSortMergeJoin
      * }}}
      *
      * ??? (true / false)
      *
      * {{{
      * def canBuildLocalHashMap(plan: LogicalPlan): Boolean = {
      *   plan.stats.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions
      * }
      * }}}
      *
      * With `plan` as `customersDS`
      * ??? (true / false)
      *
      * {{{
      * def muchSmaller(a: LogicalPlan, b: LogicalPlan): Boolean = {
      *   a.stats.sizeInBytes * 3 <= b.stats.sizeInBytes
      * }
      * }}}
      *
      * With `a` as `customerDS`, `b` as `ordersDS`
      * ??? (true / false)
      *
      * ??? (YES / NO)
      */

    customersAndOrdersDF.collect()
    customersAndOrdersDF.queryExecution.toString().contains("ShuffledHashJoin") should be(true)

    /**
      * HINT
      */

    /**
      * Row Count (rows)
      *
      * Customer: 4
      * Order: 400 = 4 * 100
      */

    /**
      * Estimated Size for All Rows (bytes)
      *
      * Customer: 144 = 36 * 4
      * Order: 9600 = 24 * 400
      */

    /**
      * Applicability of Shuffled Hash Join
      *
      * {{{
      * !conf.preferSortMergeJoin
      * }}}
      *
      * !false
      *
      * {{{
      * def canBuildLocalHashMap(plan: LogicalPlan): Boolean = {
      *   plan.stats.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions
      * }
      * }}}
      *
      * With `plan` as `customersDS`
      * 144 < 2 * 100
      *
      * {{{
      * def muchSmaller(a: LogicalPlan, b: LogicalPlan): Boolean = {
      *   a.stats.sizeInBytes * 3 <= b.stats.sizeInBytes
      * }
      * }}}
      *
      * With `a` as `customersDS`, `b` as `orderDS`
      * 144 * 3 <= 9600
      *
      * YES, it applies.
      */
  }
}
