import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.io.StdIn

object MainApp {
  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("location-clustering")
      .master("local[*]")
      //.config("spark.sql.autoBroadcastJoinThreshold", -1)
      .config("spark.sql.autoBroadcastJoinThreshold", 1)
      .config("spark.sql.join.preferSortMergeJoin", false)
      .config("spark.sql.shuffle.partitions", 10)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._
    val sc = spark.sparkContext

    val customersDS: Dataset[Customer] = ECommerce.customersDS(100)
    val orderDS: Dataset[Order] = ECommerce.ordersDS(100, customerId => 10)

    customersDS.write
      // .bucketBy(8, "id")
      // .sortBy("id")
      .mode(SaveMode.Overwrite)
      .saveAsTable("customers")

    orderDS.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("orders")

    val customersAndOrdersDF = spark.table("customers").as("cst")
      .join(spark.table("orders").as("ord"), $"cst.id" === $"ord.customer_id")
      .select($"cst.id".as("customer_id"), $"cst.name", $"ord.id".as("order_id"))

    customersAndOrdersDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable("orders_customers")

    StdIn.readLine()
    spark.close()
  }
}
