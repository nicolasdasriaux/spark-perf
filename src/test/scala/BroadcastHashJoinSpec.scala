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

class BroadcastHashJoinSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark shuffle join")
    .master("local[*]")
    .config("spark.sql.autoBroadcastJoinThreshold", 200)
    .getOrCreate()

  override def afterAll() {
    sparkSession.stop()
  }

  "Broadcast Hash Join" should "be performed when size conditions apply" in {
    /**
      * Applicability of '''Broadcast Hash Join''' (by sizes)
      *
      * (1) Look for cases that outputs `BroadcastHashJoinExec` after considering sizes
      * [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.apply()]]
      *
      * Should be applicable when
      *
      * {{{
      * canBroadcastBySizes(joinType, left, right)
      * }}}
      *
      * (2) Look at `canBroadcastBySizes` method
      *
      * {{{
      * def canBroadcastBySizes(joinType: JoinType, left: LogicalPlan, right: LogicalPlan): Boolean = {
      *    val buildLeft = canBuildLeft(joinType) && canBroadcast(left)
      *    val buildRight = canBuildRight(joinType) && canBroadcast(right)
      *    buildLeft || buildRight
      * }
      * }}}
      *
      * (3) Look at `canBroadcast` method
      *
      * Matches a plan whose output should be small enough to be used in broadcast join.
      * [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.canBroadcast]]
      *
      * {{{
      * def canBroadcast(plan: LogicalPlan): Boolean = {
      *   plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= conf.autoBroadcastJoinThreshold
      * }
      * }}}
      *
      * (4) Look at `spark.sql.autoBroadcastJoinThreshold` config
      *
      * Configures the maximum size in bytes for a table that will be broadcast to all worker
      * nodes when performing a join. By setting this value to -1 broadcasting can be disabled.
      * [[org.apache.spark.sql.internal.SQLConf.autoBroadcastJoinThreshold]]
      * [[org.apache.spark.sql.internal.SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]]
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
      * Applicability of '''Broadcast Hash Join''' (by sizes)
      *
      * [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.apply()]]
      * Look for cases that outputs `BroadcastHashJoinExec`
      *
      * {{{
      * def canBroadcast(plan: LogicalPlan): Boolean = {
      *   plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= conf.autoBroadcastJoinThreshold
      * }
      * }}}
      *
      * With plan = Customer
      * 144 < 200
      *
      * YES, it applies.
      */

    customersAndOrdersDF.explain(true)
    customersAndOrdersDF.queryExecution.toString().contains("BroadcastHashJoin") should be(true)
  }

  it should "should be performed when hint conditions apply" in {
    /**
      * Applicability of '''Broadcast Hash Join''' (by hints)
      *
      * (1) Look for cases that outputs `BroadcastHashJoinExec` when considering hints
      * [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.apply()]]
      *
      * Should be applicable when
      *
      * {{{
      * canBroadcastByHints(joinType, left, right)
      * }}}
      *
      * (2) Look at `canBroadcastByHints`
      *
      * {{{
      * private def canBroadcastByHints(joinType: JoinType, left: LogicalPlan, right: LogicalPlan): Boolean = {
      *   val buildLeft = canBuildLeft(joinType) && left.stats.hints.broadcast
      *   val buildRight = canBuildRight(joinType) && right.stats.hints.broadcast
      *   buildLeft || buildRight
      * }
      * }}}
      */

    implicit val spark: SparkSession = sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    /**
      * Rows
      *
      * Customer: 8
      * Order: 400 = 8 * 100
      */

    val customersDS = ECommerce.customersDS(8) //
    val ordersDS = ECommerce.ordersDS(8, customerId => 100)

    /**
      * Estimated Size for All Rows (bytes)
      *
      * Customer: 288 = 36 * 8
      * Order: 19200 = 24 * 800
      */

    val customersAndOrdersDF = broadcast(customersDS.as("cst"))
      .join(ordersDS.as("ord"), $"cst.id" === $"ord.customerId")
      .select($"cst.id".as("customerId"), $"cst.name", $"ord.id".as("orderId"))

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
      * With plan = Customer
      * broadcast(...)
      *
      * YES, it applies.
      */

    /**
      * Applicability of '''Broadcast Hash Join''' (by sizes)
      *
      * [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.apply()]]
      * Look for cases that outputs `BroadcastHashJoinExec`
      *
      * {{{
      * def canBroadcast(plan: LogicalPlan): Boolean = {
      *   plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= conf.autoBroadcastJoinThreshold
      * }
      * }}}
      *
      * With plan = Customer
      * 288 < 200
      *
      * NO, it doesn't apply.
      */

    customersAndOrdersDF.explain(true)
    customersAndOrdersDF.queryExecution.toString().contains("BroadcastHashJoin") should be(true)
  }
}
