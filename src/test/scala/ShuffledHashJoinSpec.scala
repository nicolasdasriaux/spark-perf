import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Estimating Size per Row
  *
  * [[org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils.getSizePerRow()]]
  * [[org.apache.spark.sql.types.DataType.defaultSize]]
  * [[org.apache.spark.sql.types.LongType.defaultSize]]
  * [[org.apache.spark.sql.types.StringType.defaultSize]]
  */

/**
  * Estimated Size per Row (bytes)
  *
  * Customer: 36 = 8 + 8 (LongType) + 20 (StringType)
  * Order: 24 = 8 + 8 (LongType) + 8 (LongType)
  */

class ShuffledHashJoinSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark shuffle join")
    .master("local[*]")
    .config("spark.sql.join.preferSortMergeJoin", false)
    .config("spark.sql.autoBroadcastJoinThreshold", 2)
    .config("spark.sql.shuffle.partitions", 100)
    .getOrCreate()

  override def afterAll() {
    sparkSession.stop()
  }

  "Shuffled Hash Join" should "be performed when conditions apply" in {
    /**
      * Applicability of '''Shuffled Hash Join'''
      *
      * [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection]]
      * [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.apply()]]
      *
      * (1) Look for cases that output `ShuffledHashJoinExec`
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
      * (2) Look at `canBuildLocalHashMap` method
      *
      * Matches a plan whose single partition should be small enough to build a hash table.
      * In other words, it's most worthwhile to hash and shuffle data than to broadcast it to all executors.
      * [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.canBuildLocalHashMap]]
      *
      * {{{
      * def canBuildLocalHashMap(plan: LogicalPlan): Boolean = {
      *   plan.stats.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions
      * }
      * }}}
      *
      * (3) Look at `muchSmaller` method
      *
      * Returns whether plan a is much smaller (3 x) than plan b.
      * [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.muchSmaller]]
      *
      * {{{
      * def muchSmaller(a: LogicalPlan, b: LogicalPlan): Boolean = {
      *   a.stats.sizeInBytes * 3 <= b.stats.sizeInBytes
      * }
      * }}}
      *
      * (4) Look at `spark.sql.join.preferSortMergeJoin` config
      *
      * When true, prefer sort merge join over shuffle hash join.
      * [[org.apache.spark.sql.internal.SQLConf.preferSortMergeJoin]]
      * [[org.apache.spark.sql.internal.SQLConf.PREFER_SORTMERGEJOIN]]
      *
      * (5) Look at `spark.sql.autoBroadcastJoinThreshold` config
      *
      * Configures the maximum size in bytes for a table that will be broadcast to all worker
      * nodes when performing a join. By setting this value to -1 broadcasting can be disabled.
      * [[org.apache.spark.sql.internal.SQLConf.autoBroadcastJoinThreshold]]
      * [[org.apache.spark.sql.internal.SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]]
      *
      * (6) Look at `spark.sql.shuffle.partitions` method
      *
      * The default number of partitions to use when shuffling data for joins or aggregations.
      * [[org.apache.spark.sql.internal.SQLConf.numShufflePartitions]]
      * [[org.apache.spark.sql.internal.SQLConf.SHUFFLE_PARTITIONS]]
      */

    implicit val spark: SparkSession = sparkSession
    import spark.implicits._

    /**
      * Rows
      *
      * Customer: 4
      * Order: 400 = 4 * 100
      */

    val customersDS = ECommerce.customersDS(4) //
    val ordersDS = ECommerce.ordersDS(4, customerId => 100)

    /**
      * Estimated Size for All Rows (bytes)
      *
      * Customer: 144 = 36 * 4
      * Order: 9600 = 24 * 400
      */

    val customersAndOrdersDF = customersDS.as("cst")
      .join(ordersDS.as("ord"), $"cst.id" === $"ord.customerId")
      .select($"cst.id".as("customerId"), $"cst.name", $"ord.id".as("orderId"))

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
      * With plan = Customer
      * 144 < 2 * 100
      *
      * {{{
      * def muchSmaller(a: LogicalPlan, b: LogicalPlan): Boolean = {
      *   a.stats.sizeInBytes * 3 <= b.stats.sizeInBytes
      * }
      * }}}
      *
      * With a = Customer, b = Order
      * 144 * 3 <= 9600
      *
      * YES, it applies.
      */

    customersAndOrdersDF.explain(true)
    customersAndOrdersDF.queryExecution.toString().contains("ShuffledHashJoin") should be(true)
  }
}
