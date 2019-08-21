import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

class JsonRead extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("JSON Read")
    .master("local[*]")
    .config("spark.default.parallelism", 8) // Default parallelism in Spark
    .config("spark.sql.shuffle.partitions", 200) // Parallelism when shuffling in Spark SQL

    .enableHiveSupport()
    .getOrCreate()

  override protected def beforeAll(): Unit = {
    implicit val spark: SparkSession = sparkSession
    import org.apache.spark.sql.functions._
    import spark.implicits._

    val rowCount = 1000000

    val customersDF = (1 to rowCount)
      .toDF("id")
      .select(
        $"id",
        concat(lit("First Name "), $"id", lit(" Last Name "), $"id").as("name")
      )

    customersDF
      .coalesce(1) // Force to write just 1 big part
      .write
      .mode(SaveMode.Overwrite)
      .json("C:\\development\\presentations\\spark-perf\\src\\test\\resources\\customers.jsonl")

    val jsonDF =
      Seq("[").toDF union
        customersDF
          .select(
            concat(
              lit("  {\n"),
              lit("    "), lit("\"id\": "), $"id", lit(",\n"),
              lit("    "), lit("\"name\": "), lit("\""), $"name", lit("\""), lit("\n"),
              lit("  }"), when($"id" < rowCount, ",").otherwise("")
            )
          ) union Seq("]").toDF

    jsonDF
      .coalesce(1) // Force to write just 1 big part
      .write
      .mode(SaveMode.Overwrite)
      .text("C:\\development\\presentations\\spark-perf\\src\\test\\resources\\customers.json")
  }

  override def afterAll() {
    SparkPerf.keepSparkUIAlive()
    sparkSession.stop()
  }

  "Reading a JSON Lines file" should "be parallelizable" in {
    implicit val spark: SparkSession = sparkSession

    val customerSchema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType, nullable = false)
      )
    )

    val customersDF = spark.read
      .schema(customerSchema) // Set schema
      .json("C:\\development\\presentations\\spark-perf\\src\\test\\resources\\customers.jsonl")

    customersDF.collect()
  }

  "Reading a JSON file" should "not be parallelizable" in {
    implicit val spark: SparkSession = sparkSession

    val customerSchema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType, nullable = false)
      )
    )

    val customersDF = spark.read
      .option("multiline", true)
      .schema(customerSchema) // Set schema
      .json("C:\\development\\presentations\\spark-perf\\src\\test\\resources\\customers.json")

    customersDF.collect()
  }

  "Reading a JSON Lines file with schema inference" should "add an additional full scan before reading" in {
    implicit val spark: SparkSession = sparkSession

    val customersDF = spark.read
      .option("samplingRatio", 1.0) // samplingRatio (default is 1.0): defines fraction of input JSON objects used for schema inferring
      // No schema set
      .json("C:\\development\\presentations\\spark-perf\\src\\test\\resources\\customers.jsonl")

    customersDF.collect()
  }
}
