import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

/**
 * Reading JSON
 *
 * (1) Run the test class.
 *     Eventually it will block in [[ReadingJsonSpec.afterAll]] on [[SparkPerf.keepSparkUIAlive()]] keeping Spark UI alive.
 *
 * (2) Open Spark UI in browser [[http://localhost:4040]]
 *
 * (3) Follow instructions for each of the test cases
 */

class ReadingJsonSpec extends FlatSpec with Matchers with BeforeAndAfterAll {
  val sparkSession: SparkSession = SparkSession.builder()
    .appName("Reading JSON")
    .master("local[*]")
    .config("spark.default.parallelism", 8) // Default parallelism in Spark
    .config("spark.sql.shuffle.partitions", 200) // Parallelism when shuffling in Spark SQL

    .enableHiveSupport()
    .getOrCreate()

  private val hdfsPath = SparkPerf.hdfsPath()

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
      .json(hdfsPath.resolve("customers.jsonl").toString)

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
      .text(hdfsPath.resolve("customers.json").toString)
  }

  override def afterAll() {
    SparkPerf.keepSparkUIAlive()
    sparkSession.stop()
  }

  "Reading a JSON Lines file" should "be parallelizable" in {
    /**
     * (4) Read about JSON Lines format [[http://jsonlines.org/]]
     *
     * (5) Take a look at `hdfs/customers.jsonl` directory
     *     - Notice single part
     *     - Notice presence of a JSON object per line without LF in the middle of line
     *
     * (6) Map **Queries** to code lines of this test using line number (and map queries to jobs)
     *     - `collect` method call line
     *
     * (7) Map **Jobs** to code lines of this test using line number
     *     - `collect` method call line
     *
     * (8) Map **Stages** to code lines of this test using line number
     *     - `collect` method call line
     *
     * (9) Identify how many tasks are used to:
     *     - Read JSON Lines file
     */

    implicit val spark: SparkSession = sparkSession

    val customerSchema = StructType(
      Seq(
        StructField("id", LongType, nullable = false),
        StructField("name", StringType, nullable = false)
      )
    )

    val customersDF = spark.read
      .schema(customerSchema) // Set schema
      .json(hdfsPath.resolve("customers.jsonl").toString)

    customersDF.collect()
  }

  "Reading a JSON file" should "not be parallelizable" in {
    /**
     * (10) Take a look at `hdfs/customers.json` directory
     *      - Notice single part
     *      - Notice content consists of a JSON array containing JSON objects (with LFs sprinkled out here and there)
     *
     * (11) Map **Queries** to code lines of this test using line number (and to which Jobs they map to)
     *      - `collect` method call line
     *
     * (12) Map **Jobs** to code lines of this test using line number
     *      - `collect` method call line
     *
     * (13) Map **Stages** to code lines of this test using line number
     *      - `collect` method call line
     *
     * (14) Identify how many tasks are used to:
     *      - Read JSON file
     */

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
      .json(hdfsPath.resolve("customers.json").toString)

    customersDF.collect()
  }

  "Reading a JSON Lines file with schema inference" should "add an additional full scan before reading" in {
    /**
     * (15) Map **Queries** to code lines of this test using line number (and to which Jobs they map to)
     *      - `collect` method call line
     *
     * (16) Map **Jobs** to code lines of this test using line number
     *      - `json` method call line
     *      - `collect` method call line
     *
     * (17) Map **Stages** to code lines of this test using line number
     *      - `json` method call line
     *      - `collect` method call line
     *
     * (18) Identify how many tasks are used and how many records are read to:
     *      - Infer schema
     *      - Read JSON Lines file
     */

    implicit val spark: SparkSession = sparkSession

    val customersDF = spark.read
      .option("samplingRatio", 1.0) // samplingRatio (default is 1.0): defines fraction of input JSON objects used for schema inferring
      // No schema set
      .json(hdfsPath.resolve("customers.jsonl").toString)

    customersDF.collect()
  }
}
