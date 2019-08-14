import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Broadcast Hash Join
  *
  * (1) Read '''Broadcast Hash Join''' section of the following page
  *     [[https://medium.com/@achilleus/https-medium-com-joins-in-apache-spark-part-3-1d40c1e51e1c]]
  *
  * (2) Optionally read the following page
  *     [[https://www.waitingforcode.com/apache-spark-sql/broadcast-join-spark-sql/read]]
  *
  * (3) Run the test class.
  *     Eventually it will block in [[BroadcastHashJoinSpec.afterAll]] on [[SparkPerf.keepSparkUIAlive()]] keeping Spark UI alive.
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

class BroadcastHashJoinSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Broadcast Hash Join")
    .master("local[*]")
    .config("spark.default.parallelism", 8) // Default parallelism in Spark
    .config("spark.sql.shuffle.partitions", 200) // Parallelism when shuffling in Spark SQL

    .config("spark.sql.autoBroadcastJoinThreshold", 200)
    .getOrCreate()

  override def afterAll() {
    SparkPerf.keepSparkUIAlive()
    sparkSession.stop()
  }

  "Broadcast Hash Join" should "be performed when size conditions apply" in {
    /**
      * Observing Physical Plan with `BroadcastHashJoin`
      *
      * (7) Observe plan for query in '''Spark UI'''
      *     - Notice `BroadcastHashJoin` node
      *     - Notice `BroadcastExchange` node (broadcast)
      */

    /**
      * Understanding Applicability of '''Broadcast Hash Join''' (by sizes)
      *
      * (8) Look for cases that outputs `BroadcastHashJoinExec` after considering '''sizes'''
      *     [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.apply()]]
      *
      * Should be applicable when
      *
      * {{{
      * canBroadcastBySizes(joinType, left, right)
      * }}}
      *
      * (9) Look at `canBroadcastBySizes` method
      *     [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.canBroadcastBySizes]]
      *
      * {{{
      * def canBroadcastBySizes(joinType: JoinType, left: LogicalPlan, right: LogicalPlan): Boolean = {
      *    val buildLeft = canBuildLeft(joinType) && canBroadcast(left)
      *    val buildRight = canBuildRight(joinType) && canBroadcast(right)
      *    buildLeft || buildRight
      * }
      * }}}
      *
      * (10) Look at `canBroadcast` method
      *      [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.canBroadcast]]
      *
      * Matches a plan whose output should be small enough to be used in broadcast join.
      *
      * {{{
      * def canBroadcast(plan: LogicalPlan): Boolean = {
      *   plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= conf.autoBroadcastJoinThreshold
      * }
      * }}}
      *
      * `plan.stats.sizeInBytes` is estimated size (in bytes) for all rows.
      *
      * (11) Look at `spark.sql.autoBroadcastJoinThreshold` config
      *      [[org.apache.spark.sql.internal.SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]]
      *      [[org.apache.spark.sql.internal.SQLConf.autoBroadcastJoinThreshold]]
      *
      * Configures the maximum size in bytes for a table that will be broadcast to all worker
      * nodes when performing a join. By setting this value to -1 broadcasting can be disabled.
      */

    implicit val spark: SparkSession = sparkSession
    import spark.implicits._

    val customersDS = ECommerce.customersWithKnownRowCountDS(4)
    val ordersDS = ECommerce.ordersWithKnownRowCountDS(4, customerId => 100)

    /**
      * (12) Determine Row Count (in rows) for `customersDS` and `ordersDS`
      *
      * `customersDS`: 4
      * `ordersDS`: 400 = 4 (customers) * 100 (orders per customer)
      *
      * (13) Manually estimate Size for All Rows (in bytes)
      *
      * `customersDS`: 144 = 36 * 4
      * `ordersDS`: 9600 = 24 * 400
      */

    val customersAndOrdersDF = customersDS.as("cst")
      .join(ordersDS.as("ord"), $"cst.id" === $"ord.customer_id")
      .select($"cst.id".as("customer_id"), $"cst.name", $"ord.id".as("order_id"))

    /**
      * Applicability of '''Broadcast Hash Join''' (by sizes)
      *
      * (14) Determine applicability of '''Broadcast Hash Join''' for `customersDS`
      *
      * {{{
      * def canBroadcast(plan: LogicalPlan): Boolean = {
      *   plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= conf.autoBroadcastJoinThreshold
      * }
      * }}}
      *
      * With `plan` as `customersDS`
      * 144 <= 200
      *
      * YES, it applies.
      * `customersDS` will be broadcast.
      */

    customersAndOrdersDF.collect()
    customersAndOrdersDF.queryExecution.toString().contains("BroadcastHashJoin") should be(true)
  }

  it should "be performed when broadcast function applied" in {
    /**
      * Understanding Applicability of '''Broadcast Hash Join''' (by hints)
      *
      * (15) Look for cases that outputs `BroadcastHashJoinExec` when considering '''hints'''
      *      [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.apply()]]
      *
      * Should be applicable when
      *
      * {{{
      * canBroadcastByHints(joinType, left, right)
      * }}}
      *
      * (16) Look at `canBroadcastByHints`
      *      [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.canBroadcastByHints]]
      *
      * {{{
      * private def canBroadcastByHints(joinType: JoinType, left: LogicalPlan, right: LogicalPlan): Boolean = {
      *   val buildLeft = canBuildLeft(joinType) && left.stats.hints.broadcast
      *   val buildRight = canBuildRight(joinType) && right.stats.hints.broadcast
      *   buildLeft || buildRight
      * }
      * }}}
      *
      * It will match either when left or right has a broadcast annotation.
      */

    implicit val spark: SparkSession = sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    val customersDS = ECommerce.customersWithKnownRowCountDS(8) //
    val ordersDS = ECommerce.ordersWithKnownRowCountDS(8, customerId => 100)

    /**
      * (17) Determine Row Count (in rows) for `customersDS` and `ordersDS`
      *
      * `customersDS`: ???
      * `ordersDS`: ???
      *
      * (18) Manually estimate Size for All Rows (in bytes)
      *
      * `customersDS`: ???
      * `ordersDS`: ???
      */

    val broadcastCustomersDS = broadcast(customersDS)

    val customersAndOrdersDF = broadcastCustomersDS.as("cst")
      .join(ordersDS.as("ord"), $"cst.id" === $"ord.customer_id")
      .select($"cst.id".as("customer_id"), $"cst.name", $"ord.id".as("order_id"))

    /**
      * Applicability of '''Broadcast Hash Join''' (by hints)
      *
      * (19) Determine applicability of '''Broadcast Hash Join''' (by hints)
      *
      * {{{
      * private def canBroadcastByHints(joinType: JoinType, left: LogicalPlan, right: LogicalPlan): Boolean = {
      *   val buildLeft = canBuildLeft(joinType) && left.stats.hints.broadcast
      *   val buildRight = canBuildRight(joinType) && right.stats.hints.broadcast
      *   buildLeft || buildRight
      * }
      * }}}
      *
      * With `plan` as `CustomerDS`
      * ??? (true / false)
      *
      * ??? (YES / NO)
      */

    /**
      * Applicability of '''Broadcast Hash Join''' (by sizes)
      *
      * (20) Determine applicability of '''Broadcast Hash Join''' (by sizes)
      *
      * {{{
      * def canBroadcast(plan: LogicalPlan): Boolean = {
      *   plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= conf.autoBroadcastJoinThreshold
      * }
      * }}}
      *
      * With `plan` as `CustomerDS`
      * ??? (true / false)
      *
      * ??? (YES / NO)
      */

    customersAndOrdersDF.collect()
    customersAndOrdersDF.queryExecution.toString().contains("BroadcastHashJoin") should be(true)

    /**
      * HINTS
      */

    /**
      * Row Count (rows)
      *
      * Customer: 8
      * Order: 800 = 8 * 100
      **/

    /**
      * Estimated Size for All Rows (bytes)
      *
      * Customer: 288 = 36 * 8
      * Order: 19200 = 24 * 800
      */

    /**
      * Applicability of '''Broadcast Hash Join''' (by hints)
      *
      * {{{
      * private def canBroadcastByHints(joinType: JoinType, left: LogicalPlan, right: LogicalPlan): Boolean = {
      *   val buildLeft = canBuildLeft(joinType) && left.stats.hints.broadcast
      *   val buildRight = canBuildRight(joinType) && right.stats.hints.broadcast
      *   buildLeft || buildRight
      * }
      * }}}
      *
      * With plan for `broadcastCustomersDS`
      * true
      *
      * YES, it applies.
      */

    /**
      * Applicability of '''Broadcast Hash Join''' (by sizes)
      *
      * {{{
      * def canBroadcast(plan: LogicalPlan): Boolean = {
      *   plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= conf.autoBroadcastJoinThreshold
      * }
      * }}}
      *
      * With plan for `broadcastCustomersDS`
      * 288 < 200
      *
      * NO, it doesn't apply.
      */
  }

  it should "be performed when broadcast hint is applied" in {
    implicit val spark: SparkSession = sparkSession
    import spark.implicits._

    val customersDS = ECommerce.customersWithKnownRowCountDS(8) //
    val ordersDS = ECommerce.ordersWithKnownRowCountDS(8, customerId => 100)

    /**
      * (21) Notice alternative way of adding a broadcast hint
      */
    val broadcastCustomersDS = customersDS.hint("broadcast")

    val customersAndOrdersDF = broadcastCustomersDS.as("cst")
      .join(ordersDS.as("ord"), $"cst.id" === $"ord.customer_id")
      .select($"cst.id".as("customer_id"), $"cst.name", $"ord.id".as("order_id"))

    customersAndOrdersDF.collect()
    customersAndOrdersDF.queryExecution.toString().contains("BroadcastHashJoin") should be(true)
  }
}
