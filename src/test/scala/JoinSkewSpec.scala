import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

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
    implicit val spark: SparkSession = sparkSession
    import spark.implicits._

    val customersDS = ECommerce.customersDS(10)
    val ordersDS = ECommerce.ordersDS(10, customerId => if (customerId == 1) 1000000 else 5)

    val customersAndOrdersDF = customersDS.as("cst")
      .join(ordersDS.as("ord"), $"cst.id" === $"ord.customerId")
      .select($"cst.id".as("customerId"), $"cst.name", $"ord.id".as("orderId"))

    customersAndOrdersDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("customers_orders_skew")
  }

  it should "be fixable with broadcasting" in {
    implicit val spark: SparkSession = sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val customersDS = ECommerce.customersDS(10)
    val ordersDS = ECommerce.ordersDS(10, customerId => if (customerId == 1) 1000000 else 5)

    val broadcastCustomersDS = broadcast(customersDS)

    val customersAndOrdersDF = broadcastCustomersDS.as("cst")
      .join(ordersDS.as("ord"), $"cst.id" === $"ord.customerId")
      .select($"cst.id".as("customerId"), $"cst.name", $"ord.id".as("orderId"))

    customersAndOrdersDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("customers_orders_broadcast")
  }

  it should "be fixable with salting" in {
    implicit val spark: SparkSession = sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val customersDS = ECommerce.customersDS(10)
    val ordersDS = ECommerce.ordersDS(10, customerId => if (customerId == 1) 1000000 else 5)

    val saltCount = 200
    val saltedCustomersDF = customersDS.withColumn("salt", explode(lit((0 until saltCount).toArray)))
    val saltedOrdersDF = ordersDS.withColumn("salt", round(rand * (saltCount - 1)).cast(IntegerType))

    val customersAndOrdersDF = saltedCustomersDF.as("cst")
      .join(saltedOrdersDF.as("ord"), $"cst.id" === $"ord.customerId" && $"cst.salt" === $"ord.salt")
      .select($"cst.id".as("customerId"), $"cst.name", $"ord.id".as("orderId"))

    customersAndOrdersDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("customers_orders_salt")
  }
}
