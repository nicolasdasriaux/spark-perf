import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.apache.spark.sql.types.IntegerType
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Partitioning
  *
  * (1) Read '''Parquet Partitioning''', '''Partition Pruning and Predicate Pushdown''' and '''Partitioning Tips''' sections of the following page
  *     [[https://mapr.com/blog/tips-and-best-practices-to-take-advantage-of-spark-2-x/]]
  *
  * (2) Run the test class.
  *     Eventually it will block in [[PartitioningSpec.afterAll]] on [[SparkPerf.keepSparkUIAlive()]] keeping Spark UI alive.
  *
  * (3) Open Spark UI in browser [[http://localhost:4040]]
  *
  * (4) Follow instructions for each of the test cases
  */

class PartitioningSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Partitioning")
    .master("local[*]")
    .config("spark.default.parallelism", 8) // Default parallelism in Spark
    .config("spark.sql.shuffle.partitions", 200) // Parallelism when shuffling in Spark SQL

    .enableHiveSupport()
    .getOrCreate()

  override def afterAll() {
    SparkPerf.keepSparkUIAlive()
    sparkSession.stop()
  }

  "Absence of Partitioning" should "not allow to avoid filtering" in {
    /**
      * Observing Physical Plan when absence of partitioning
      *
      * (5) Observe plan for query in '''Spark UI'''
      *     - Presence of `Scan` node fully reading the table
      *     - Presence of `Filter` node
      */

    implicit val spark: SparkSession = sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    def country(id: Column): Column =
      element_at(lit(Array("France", "Germany", "Portugal", "Spain", "Italy")), id)

    val countryCustomersDF = ECommerce.customersDS(1000000)
      .withColumn("country", country(($"id" % 5 + 1).cast(IntegerType)))

    countryCustomersDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("country_customers_no_partition")

    val franceCustomersDF = spark.table("country_customers_no_partition")
      .where($"country" === "France")

    franceCustomersDF.collect()
    franceCustomersDF.queryExecution.toString() shouldNot include ("PartitionCount: 1")
  }

  "Partitioning" should "allow to avoid filtering" in {
    /**
      * Observing Storage Structure of partitioned table
      *
      * (6) Study the code below that does '''Partitioned Table Creation'''
      *
      * (7) Observe structure of saved `country_customers_partition` table
      *     - Look at `spark-warehouse/country_customers_partition` folder
      *     - Observe sub-directories such as `country=France` containing rows for a partition
      *
      *     - Observe `part-PPPPP-xxxxxxxxxxxxxxxx.xxxx.snappy.parquet` files inside those sub-directories
      *       - PPPPP is part number
      */

    /**
      * Observing Physical Plan when presence of partitioning
      *
      * (8) Observe plan for query in '''Spark UI'''
      *     - Observe early '''partition pruning''' directly in the `Scan` node
      *     - Notice the absence of `Filter` node
      */

    implicit val spark: SparkSession = sparkSession
    import spark.implicits._
    import org.apache.spark.sql.functions._

    def country(id: Column): Column =
      element_at(lit(Array("France", "Germany", "Portugal", "Spain", "Italy")), id)

    val countryCustomersDF = ECommerce.customersDS(1000000)
      .withColumn("country", country(($"id" % 4 + 1).cast(IntegerType)))

    // Partitioned Table Creation
    countryCustomersDF.write
      .mode(SaveMode.Overwrite)
      .partitionBy("country")
      .saveAsTable("country_customers_partition")

    val franceCustomersDF = spark.table("country_customers_partition")
      .where($"country" === "France")

    franceCustomersDF.collect()
    franceCustomersDF.queryExecution.toString() should include ("PartitionCount: 1")
  }
}
