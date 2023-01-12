from pyspark.sql import SparkSession

def main(args):
    spark = SparkSession.builder().appName("HiveToGreenplum").getOrCreate()
    hiveTableName = args[0]
    greenplumTableName = args[1]
    greenplumSchema = args[2]
    greenplumURL = args[3]
    greenplumUser = args[4]
    greenplumPassword = args[5]
    
    # Read the Hive table in ORC format
    hiveTableDF = spark.read.format("orc").load(hiveTableName)
    
    # Cast the data types
    castedDF = hiveTableDF.selectExpr(
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
    
    # Create the Greenplum external table
    castedDF.write.format("io.pivotal.greenplum.spark.GreenplumDataSource")
      .option("url", greenplumURL)
      .option("user", greenplumUser)
      .option("password", greenplumPassword)
      .option("dbschema", greenplumSchema)
      .option("table", greenplumTableName)
      .mode("overwrite")
      .save()
      
    # Upload the table to Greenplum
    castedDF.write.format("io.pivotal.greenplum.spark.GreenplumRelationProvider")
      .option("url", greenplumURL)
      .option("user", greenplumUser)
      .option("password", greenplumPassword)
      .option("dbschema", greenplumSchema)
      .option("table", greenplumTableName)
      .mode("overwrite")
      .save()
      
    spark.stop()
