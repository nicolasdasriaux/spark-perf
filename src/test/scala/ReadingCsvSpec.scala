import org.apache.spark.sql.types._
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class ReadingCsvSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Reading CSV")
    .master("local[*]")
    .config("spark.default.parallelism", 8) // Default parallelism in Spark
    .config("spark.sql.shuffle.partitions", 200) // Parallelism when shuffling in Spark SQL

    .enableHiveSupport()
    .getOrCreate()

  private val hdfsPath = SparkPerf.hdfsPath()

  override def beforeAll() {
    implicit val spark: SparkSession = sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val rowCount = 2000000

    (1 to rowCount)
      .toDF("id")
      .select($"id", concat(lit("First Name "), $"id", lit(" Last Name "), $"id").as("name"))
      .coalesce(1) // Force to write just 1 big part
      .write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .csv(hdfsPath.resolve("customers.csv").toString)


    (1 to rowCount)
      .toDF("id")
      .select($"id", concat(lit("First Name "), $"id", lit("\nLast Name "), $"id").as("name"))
      .coalesce(1) // Force to write just 1 big part
      .write
      .option("header", true)
      .mode(SaveMode.Overwrite)
      .csv(hdfsPath.resolve("customers-multiline.csv").toString)
  }

  override def afterAll() {
    SparkPerf.keepSparkUIAlive()
    sparkSession.stop()
  }

  "Reading a non-multiline CSV" should "be parallelizable" in {
    implicit val spark: SparkSession = sparkSession

    val customerSchema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType, nullable = false)
      )
    )

    val customersDF = spark.read
      .option("header", true)
      .schema(customerSchema) // Set schema
      .csv(hdfsPath.resolve("customers.csv").toString)

    customersDF.collect()
  }

  "Reading a multiline CSV" should "not be parallelizable" in {
    implicit val spark: SparkSession = sparkSession

    val customerSchema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType, nullable = false)
      )
    )

    val customersDF = spark.read
      .option("header", true)
      .option("multiline", true)
      .schema(customerSchema) // Set schema
      .csv(hdfsPath.resolve("customers-multiline.csv").toString)

    customersDF.collect()
  }

  "Reading a CSV with schema inference" should "add an additional full scan before reading" in {
    implicit val spark: SparkSession = sparkSession

    val customersDF = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .option("samplingRatio", 1.0) // samplingRatio (default is 1.0): defines fraction of rows used for schema inferring
      .csv(hdfsPath.resolve("customers.csv").toString)

    customersDF.collect()
  }
}
