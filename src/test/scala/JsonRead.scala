import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class JsonRead extends FlatSpec with Matchers with BeforeAndAfterAll {
    val sparkSession: SparkSession = SparkSession.builder()
      .appName("JSON Read")
      .master("local[*]")
      .config("spark.default.parallelism", 8) // Default parallelism in Spark
      .config("spark.sql.shuffle.partitions", 200) // Parallelism when shuffling in Spark SQL

      .enableHiveSupport()
      .getOrCreate()

    override def afterAll()
    {
      SparkPerf.keepSparkUIAlive()
      sparkSession.stop()
    }

    "" should "" in {
      implicit val spark: SparkSession = sparkSession
      import spark.implicits._
      import org.apache.spark.sql.functions._
    }
  }
