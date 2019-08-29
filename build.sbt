val root = (project in file(".")).settings(
  name := "spark-perf",
  version := "0.1",
  scalaVersion := "2.11.12",

  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.4.3",
    "org.apache.spark" %% "spark-hive" % "2.4.3",

    "org.scalatest" %% "scalatest" % "3.0.5" % Test,
    "com.holdenkarau" %% "spark-testing-base" % "2.4.3_0.12.0" % Test
  ),

  Test / fork := true,
  run / fork := true,
  Test / parallelExecution := false,
  javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled")
)
