import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SortMergeJoinSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Sort Merge Join")
    .master("local[*]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .config("spark.sql.join.preferSortMergeJoin", false)
    .getOrCreate()

  override def afterAll() {
    sparkSession.stop()
  }

  "Sort Merge Join" should "be performed when conditions apply" in {
    /**
      * Applicability of '''Sort Merge Join'''
      *
      * [[org.apache.spark.sql.execution.SparkStrategies.JoinSelection.apply()]]
      *
      * (1) Look for cases that output `SortMergeJoinExec`
      * See how '''Broadcast Hash Join''' and then '''Shuffled Hash Join''' are tested for applicability
      * before attempting with '''Sort Merge Join'''
      *
      * (2) Look at `spark.sql.join.preferSortMergeJoin` config
      * See how to disable '''Shuffled Hash Join''' (unless '''Sort Merge Join''' is not applicable)
      *
      * When true, prefer sort merge join over shuffle hash join.
      * [[org.apache.spark.sql.internal.SQLConf.preferSortMergeJoin]]
      * [[org.apache.spark.sql.internal.SQLConf.PREFER_SORTMERGEJOIN]]
      *
      * (3) Look at `spark.sql.autoBroadcastJoinThreshold` config
      * See how to disable '''Broadcast Hash Join''' (by sizes)
      *
      * Configures the maximum size in bytes for a table that will be broadcast to all worker
      * nodes when performing a join. By setting this value to -1 broadcasting can be disabled.
      * [[org.apache.spark.sql.internal.SQLConf.autoBroadcastJoinThreshold]]
      * [[org.apache.spark.sql.internal.SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]]
      */

    implicit val spark: SparkSession = sparkSession
    import spark.implicits._

    val customersDS = ECommerce.customersDS(4) //
    val ordersDS = ECommerce.ordersDS(4, customerId => 100)

    val customersAndOrdersDF = customersDS.as("cst")
      .join(ordersDS.as("ord"), $"cst.id" === $"ord.customerId")
      .select($"cst.id".as("customerId"), $"cst.name", $"ord.id".as("orderId"))

    customersAndOrdersDF.explain(true)
    customersAndOrdersDF.queryExecution.toString().contains("SortMergeJoin") should be(true)
  }
}
