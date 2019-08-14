import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Join Skew (and data skew in general)
  *
  * (1) Read the following page
  *     [[https://dzone.com/articles/why-your-spark-apps-are-slow-or-failing-part-ii-da]]
  *
  * (2) Run the test class.
  *     Eventually it will block in [[JoinSkewSpec.afterAll]] on [[SparkPerf.keepSparkUIAlive()]] keeping Spark UI alive.
  *
  * (3) Open Spark UI in browser [[http://localhost:4040]]
  *
  * (4) Follow instructions for each of the test cases
  */

class JoinSkewSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Join Skew")
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

  "Join Skew" should "be observable when unbalanced join" in {
    /**
      * (5) Notice how orders are highly unbalanced over customers
      *     Customer #0 has 1,000,000 orders whereas customers #2 to #9 have only 5 orders
      *
      * (6) View timeline of join stage
      *     - Identify Job and stage containing `SortMergeJoin`
      *     - View details of stage
      *     - Using Event Timeline, notice how one of the tasks takes much longer than any other
      *     - Find other evidences using Summary Metrics table and also Tasks table (size, number of records)
      */

    implicit val spark: SparkSession = sparkSession
    import spark.implicits._

    val customersDS = ECommerce.customersDS(10)
    val ordersDS = ECommerce.ordersDS(10, customerId => if (customerId == 1) 1000000 else 5)

    val customersAndOrdersDF = customersDS.as("cst")
      .join(ordersDS.as("ord"), $"cst.id" === $"ord.customer_id")
      .select($"cst.id".as("customer_id"), $"cst.name", $"ord.id".as("order_id"))

    customersAndOrdersDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("customers_orders_skew")
  }

  it should "be fixable with salting" in {
    /**
      * (7) Study the code below that does Salting (specifically additional `salt` columns)
      * (8) Study the code below that does a Salted Join (specifically use of `salt` columns in join)
      *
      * (9) View timeline of join stage
      *     - Identify Job and stage containing `SortMergeJoin`
      *     - View details of stage
      *     - Using Event Timeline, notice how tasks are now much more balanced
      *     - Find other evidences of this improvement
      */

    implicit val spark: SparkSession = sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val customersDS = ECommerce.customersDS(10)
    val ordersDS = ECommerce.ordersDS(10, customerId => if (customerId == 1) 1000000 else 5)

    // Salting
    val saltCount = 100
    val saltedCustomersDF = customersDS.withColumn("salt", explode(lit((0 until saltCount).toArray)))
    val saltedOrdersDF = ordersDS.withColumn("salt", round(rand * (saltCount - 1)).cast(IntegerType))

    // Salted Join
    val customersAndOrdersDF = saltedCustomersDF.as("cst")
      .join(saltedOrdersDF.as("ord"), $"cst.id" === $"ord.customer_id" && $"cst.salt" === $"ord.salt")
      .select($"cst.id".as("customer_id"), $"cst.name", $"ord.id".as("order_id"))

    customersAndOrdersDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("customers_orders_salt")
  }
}
