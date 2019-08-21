import java.nio.file.{Path, Paths}

import scala.io.StdIn

object SparkPerf {
  def keepSparkUIAlive(): Unit = StdIn.readLine()
  def hdfsPath(): Path = Paths.get(getClass.getResource("/").toURI).getParent.getParent.getParent.resolve("hdfs")
}
