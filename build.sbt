name := "HiveToGreenplum"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

libraryDependencies += "io.pivotal.greenplum" %% "greenplum-spark" % "1.0.0"
