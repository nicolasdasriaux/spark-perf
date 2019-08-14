import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}

object ECommerce {
  def customersWithKnownRowCountDS(customerCount: Long)(implicit spark: SparkSession): Dataset[Customer] = {
    import spark.implicits._

    val customers = (0L until customerCount)
      .map(orderId => Customer(orderId, s"Name $orderId"))

    customers.toDS
  }

  def ordersWithKnownRowCountDS(customerCount: Long, orderCountByCustomerId: Long => Long)(implicit spark: SparkSession): Dataset[Order] = {
    import spark.implicits._

    val orders = (0L until customerCount)
      .flatMap { customerId =>
        val orderCount = orderCountByCustomerId(customerId)
        val startOrderId = 10000000 * customerId
        val endOrderId = startOrderId + orderCount
        (startOrderId until endOrderId).map(orderId => Order(orderId, customerId))
      }

    orders.toDS
  }

  def customersDS(customerCount: Long)(implicit spark: SparkSession): Dataset[Customer] = {
    implicit val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    val customersRDD = sc.parallelize(0L until customerCount)
      .map(orderId => Customer(orderId, s"Name $orderId"))

    customersRDD.toDS
  }

  def ordersDS(customerCount: Long, orderCountByCustomerId: Long => Long)(implicit spark: SparkSession): Dataset[Order] = {
    implicit val sc: SparkContext = spark.sparkContext
    import spark.implicits._

    val ordersRDD = sc.parallelize(0L until customerCount)
      .flatMap { customerId =>
        val orderCount = orderCountByCustomerId(customerId)
        val startOrderId = 10000000 * customerId
        val endOrderId = startOrderId + orderCount
        (startOrderId until endOrderId).map(orderId => Order(orderId, customerId))
      }

    ordersRDD.toDS
  }
}
