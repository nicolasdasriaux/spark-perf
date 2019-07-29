import org.apache.spark.sql.SparkSession

object SparkSessionExtensions {
  implicit class SparkSessionExt(val `this`: SparkSession.Builder) {
    def autoBroadcastJoinThreshold(value: Int): SparkSession.Builder = `this`.config("spark.sql.autoBroadcastJoinThreshold", value)
    def preferSortMergeJoin(flag: Boolean): SparkSession.Builder = `this`.config("spark.sql.join.preferSortMergeJoin", flag)
  }
}
