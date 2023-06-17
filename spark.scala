import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

import java.sql.{Connection, DriverManager}

object HiveToGreenplum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HiveToGreenplum").getOrCreate()
    val hiveTableName = args(0)
    val greenplumTableName = args(1)
    val greenplumURL = args(2)
    val greenplumUser = args(3)
    val greenplumPassword = args(4)
    val useExternalTable = args(5).toBoolean // true - use external table, false - direct write
    val externalTableName = args(6)

    // JDBC URL for Greenplum
    val greenplumJdbcUrl = s"jdbc:postgresql://$greenplumURL?user=$greenplumUser&password=$greenplumPassword"

    // Method to convert schema to SQL
    def schemaToSql(schema: StructType, tableName: String): String = {
      val columns = schema.fields.map { field =>
        val name = field.name
        val typeStr = field.dataType match {
          case _: ByteType     => "INT"
          case _: ShortType    => "INT"
          case _: IntegerType  => "INT"
          case _: LongType     => "BIGINT"
          case _: FloatType    => "REAL"
          case _: DoubleType   => "DOUBLE PRECISION"
          case _: StringType   => "TEXT"
          case _: BinaryType   => "BYTEA"
          case _: BooleanType  => "BOOLEAN"
          case _: TimestampType => "TIMESTAMP"
          case _: DateType     => "DATE"
          case _               => "TEXT"  // default to TEXT
        }
        s"$name $typeStr"
      }
      s"CREATE TABLE $tableName (${columns.mkString(", ")})"
    }

    try {
      // Read the Hive table
      val hiveTableDF = spark.read.table(hiveTableName)

      // Automatically detect and cast the data types
      val castedDF = hiveTableDF.select(hiveTableDF.columns.map { colName =>
        val colType = hiveTableDF.schema(colName).dataType
        colType match {
          case _: ByteType      => hiveTableDF(colName).cast(IntegerType).as(colName)
          case _: ShortType     => hiveTableDF(colName).cast(IntegerType).as(colName)
          case _: BinaryType    => hiveTableDF(colName).cast(StringType).as(colName)
          case _                => hiveTableDF(colName)
        }
      }: _*)

      // Create table schema in Greenplum
      val createTableSQL = schemaToSql(castedDF.schema, greenplumTableName)
      val insertDataSQL = s"INSERT INTO $greenplumTableName SELECT * FROM $externalTableName"

      val cone: Connection = DriverManager.getConnection(greenplumJdbcUrl)

      try {
        val stmt = cone.createStatement()
        stmt.execute(insertDataSQL)
      } catch {
        case e: Exception =>
          println(s"Error while loading data from external table to Greenplum: ${e.getMessage}")
          e.printStackTrace()
      } finally {
        cone.close()
      }

      val conn: Connection = DriverManager.getConnection(greenplumJdbcUrl)
      try {
        val stmt = conn.createStatement()
        stmt.execute(createTableSQL)
      } catch {
        case e: Exception =>
          println(s"Error while creating table schema in Greenplum: ${e.getMessage}")
          e.printStackTrace()
      } finally {
        conn.close()
      }

      if (useExternalTable) {
        // Create the Greenplum external table
        castedDF.write.format("io.pivotal.greenplum.spark.GreenplumDataSource")
          .option("url", greenplumURL)
          .option("user", greenplumUser)
          .option("password", greenplumPassword)
          .option("table", externalTableName)
          .mode("overwrite")
          .save()

        println(s"Successfully created external table $externalTableName")
      } else {
        // Write data directly to Greenplum
        castedDF.repartition(20).write // repartition data into 20 partitions for parallel writing
          .format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
          .option("url", greenplumURL)
          .option("user", greenplumUser)
          .option("password", greenplumPassword)
          .option("table", greenplumTableName)
          .mode("overwrite")
          .save()

        println(s"Successfully wrote data to $greenplumTableName")
      }

      // Data integrity check
      val greenplumCount = spark.read.format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
        .option("url", greenplumURL)
        .option("user", greenplumUser)
        .option("password", greenplumPassword)
        .option("table", greenplumTableName)
        .load()
        .count()

      val hiveCount = hiveTableDF.count()

      if (greenplumCount == hiveCount) {
        println("Data integrity check passed")
      } else {
        println("Data integrity check failed")
      }

    } catch {
      case e: Exception =>
        println(s"An error occurred during the migration: ${e.getMessage}")
        e.printStackTrace()
    } finally {
      spark.stop()
    }
  }
}
