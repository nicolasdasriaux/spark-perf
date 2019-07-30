import org.apache.spark.sql.{Dataset, SparkSession}

object ECommerce {
  def customersDS(customerCount: Long)(implicit spark: SparkSession): Dataset[Customer] = {
    import spark.implicits._

    val customers = (0l until customerCount)
      .map(orderId => Customer(orderId, s"Name $orderId"))

    customers.toDS
  }

  def ordersDS(customerCount: Long, orderCountByCustomerId: Long => Long)(implicit spark: SparkSession): Dataset[Order] = {
    import spark.implicits._

    val orders = (0l until customerCount)
      .flatMap { customerId =>
        val orderCount = orderCountByCustomerId(customerId)
        val startOrderId = 10000000 * customerId
        val endOrderId = startOrderId + orderCount
        (startOrderId until endOrderId).map(orderId => Order(orderId, customerId))
      }

    orders.toDS
  }
}
