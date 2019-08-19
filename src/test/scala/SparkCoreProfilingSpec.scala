import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class SparkCoreProfilingSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Spark Core Profiling")
    .master("local[*]")
    .config("spark.default.parallelism", 8) // Default parallelism in Spark
    .config("spark.sql.shuffle.partitions", 200) // Parallelism when shuffling in Spark SQL

    .enableHiveSupport()
    .getOrCreate()

  override def afterAll() {
    SparkPerf.keepSparkUIAlive()
    sparkSession.stop()
  }

  "Spark Core RDD" should "appear in Job tab" in {
    implicit val sc: SparkContext = sparkSession.sparkContext

    def group(i: Int): Int = i % 10
    def square(i: Int): Int = i * i

    val numbersRDD = sc.parallelize(1 to 1000)

    val sumOfSquaresByGroupRDD = numbersRDD
      .map(number => group(number) -> square(number))
      .reduceByKey(_ + _)

    sumOfSquaresByGroupRDD.collect()
  }
}
