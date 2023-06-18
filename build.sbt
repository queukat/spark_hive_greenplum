name := "HiveToGreenplum"

version := "0.1"

scalaVersion := "2.13.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.0",
  "org.apache.spark" %% "spark-sql" % "3.4.0",
  "org.postgresql" % "postgresql" % "42.2.5",
  "org.scalatest" %% "scalatest" % "3.2.9" % Test,
  "com.typesafe" % "config" % "1.4.1"
)
