import org.apache.spark.sql.{Column, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SparkSqlProfilingSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark SQL Profiling")
    .master("local[*]")
    .config("spark.default.parallelism", 8) // Default parallelism in Spark
    .config("spark.sql.shuffle.partitions", 200) // Parallelism when shuffling in Spark SQL

    .enableHiveSupport()
    .getOrCreate()

  override def afterAll() {
    SparkPerf.keepSparkUIAlive()
    sparkSession.stop()
  }

  "Spark SQL Query" should "appear in SQL tab" in {
    implicit val spark: SparkSession = sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._

    def group(i: Column): Column = i % 10
    def square(i: Column): Column = i * i

    val numbersDF = (1 to 1000).toDF("number")

    val sumOfSquaresByGroupDF = numbersDF
      .select(group($"number").as("group"), square($"number").as("square"))
      .groupBy($"group")
      .agg(sum($"square").as("sum_of_squares"))

    sumOfSquaresByGroupDF.collect()
  }
}
