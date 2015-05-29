
name := "spark-labs"

version := "0.0.1"

scalaVersion := "2.10.5"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.0" % "provided",
  "org.apache.spark" %% "spark-sql" % "1.3.0",
  "com.databricks" %% "spark-csv" % "1.0.3",
  "org.scalatest" %% "scalatest" % "2.2.1" % "test"
)
