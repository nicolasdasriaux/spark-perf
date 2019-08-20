import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class CsvReadSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("CSV Read")
    .master("local[*]")
    .config("spark.default.parallelism", 8) // Default parallelism in Spark
    .config("spark.sql.shuffle.partitions", 200) // Parallelism when shuffling in Spark SQL

    .enableHiveSupport()
    .getOrCreate()

  override def beforeAll() {
    implicit val spark: SparkSession = sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._

    (1 to 2000000)
      .toDF("id")
      .select($"id", concat(lit("First Name "), $"id", lit(" Last Name "), $"id").as("name"))
      .coalesce(1) // Force to write just 1 big part
      .write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .csv("C:\\development\\presentations\\spark-perf\\src\\test\\resources\\customers.csv")


    (1 to 2000000)
      .toDF("id")
      .select($"id", concat(lit("First Name "), $"id", lit("\nLast Name "), $"id").as("name"))
      .coalesce(1)
      .write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .csv("C:\\development\\presentations\\spark-perf\\src\\test\\resources\\customers-multiline.csv")
  }

  override def afterAll() {
    SparkPerf.keepSparkUIAlive()
    sparkSession.stop()
  }

  "Reading rom a non-multiline CSV" should "be parallelizable" in {
    implicit val spark: SparkSession = sparkSession

    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType, nullable = false)
      )
    )

    spark.read
      .option("header", true)
      .schema(schema)
      .csv("C:\\development\\presentations\\spark-perf\\src\\test\\resources\\customers.csv")
      .collect()
  }

  "Reading from a multiline CSV" should "not be parallelizable" in {
    implicit val spark: SparkSession = sparkSession

    val schema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType, nullable = false)
      )
    )

    spark.read
      .option("header", true)
      .option("multiline", true)
      .schema(schema)
      .csv("C:\\development\\presentations\\spark-perf\\src\\test\\resources\\customers-multiline.csv")
      .collect()
  }

  "Reading from CSV with schema inference" should "add an additional full scan before reading" in {
    implicit val spark: SparkSession = sparkSession

    spark.read
      .option("header", true)
      .option("inferSchema", true)
      .option("samplingRatio", 1.0) // samplingRatio (default is 1.0): defines fraction of rows used for schema inferring.
      .csv("C:\\development\\presentations\\spark-perf\\src\\test\\resources\\customers.csv")
      .collect()
  }
}
