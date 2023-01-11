import org.apache.spark.sql.SparkSession

object HiveToGreenplum {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("HiveToGreenplum").getOrCreate()
    val hiveTableName = args(0)
    val greenplumTableName = args(1)
    val greenplumSchema = args(2)
    val greenplumURL = args(3)
    val greenplumUser = args(4)
    val greenplumPassword = args(5)
    
    // Read the Hive table in ORC format
    val hiveTableDF = spark.read.format("orc").load(hiveTableName)
    
    // Cast the data types
    val castedDF = hiveTableDF.selectExpr(
      "cast(boolean_column as bool) as boolean_column",
      "cast(int_column as int4) as int_column",
      "cast(smallint_column as int2) as smallint_column",
      "cast(tinyint_column as int2) as tinyint_column",
      "cast(bigint_column as int8) as bigint_column",
      "cast(float_column as float4) as float_column",
      "cast(double_column as float8) as double_column",
      "cast(string_column as text) as string_column",
      "cast(binary_column as bytea) as binary_column",
      "cast(timestamp_column as timestamp) as timestamp_column"
    )
    
    // Create the Greenplum external table
    castedDF.write.format("io.pivotal.greenplum.spark.GreenplumDataSource")
      .option("url", greenplumURL)
      .option("user", greenplumUser)
      .option("password", greenplumPassword)
      .option("dbschema", greenplumSchema)
      .option("table", greenplumTableName)
      .mode("overwrite")
      .save()
      
    // Upload the table to Greenplum
    castedDF.write.format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
      .option("url", greenplumURL)
      .option("user", greenplumUser)
      .option("password", greenplumPassword)
      .option("dbschema", greenplumSchema)
      .option("table", greenplumTableName)
      .mode("overwrite")
      .save()
      
    spark.stop()
  }
}
