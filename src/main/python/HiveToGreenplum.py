import configparser
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import psycopg2

def schema_to_sql(schema, table_name):
    type_mapping = {
        ByteType: "INT",
        ShortType: "INT",
        IntegerType: "INT",
        LongType: "BIGINT",
        FloatType: "REAL",
        DoubleType: "DOUBLE PRECISION",
        StringType: "TEXT",
        BinaryType: "BYTEA",
        BooleanType: "BOOLEAN",
        TimestampType: "TIMESTAMP",
        DateType: "DATE"
    }
    columns = [f"{field.name} {type_mapping.get(type(field.dataType), 'TEXT')}" for field in schema.fields]
    return f"CREATE TABLE {table_name} ({', '.join(columns)})"

def main():
    spark = SparkSession.builder.appName("HiveToGreenplum").getOrCreate()

    config = configparser.ConfigParser()
    config.read('application.conf')

    hive_table_name = config.get("hiveToGreenplum", "hiveTableName")
    greenplum_table_name = config.get("hiveToGreenplum", "greenplum_table_name")
    greenplum_url = config.get("hiveToGreenplum", "greenplum_url")
    greenplum_user = config.get("hiveToGreenplum", "greenplum_user")
    greenplum_password = config.get("hiveToGreenplum", "greenplum_password")
    use_external_table = config.getboolean("hiveToGreenplum", "useExternalTable")
    external_table_name = config.get("hiveToGreenplum", "external_table_name")

    greenplum_jdbc_url = f"jdbc:postgresql://{greenplum_url}?user={greenplum_user}&password={greenplum_password}"

    try:
        hive_table_df = spark.read.table(hive_table_name)
        casted_df = hive_table_df.select([hive_table_df[col_name].cast(IntegerType()).alias(col_name) if type(hive_table_df.schema[col_name].dataType) in (ByteType, ShortType, BinaryType) else hive_table_df[col_name] for col_name in hive_table_df.columns])

        create_table_sql = schema_to_sql(casted_df.schema, greenplum_table_name)
        insert_data_sql = f"INSERT INTO {greenplum_table_name} SELECT * FROM {external_table_name}"

        with psycopg2.connect(greenplum_jdbc_url) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(insert_data_sql)
                except Exception as e:
                    print(f"Error while loading data from external table to Greenplum: {str(e)}")
                    raise e

        with psycopg2.connect(greenplum_jdbc_url) as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute(create_table_sql)
                except Exception as e:
                    print(f"Error while creating table schema in Greenplum: {str(e)}")
                    raise e

        if use_external_table:
            casted_df.write.format("io.pivotal.greenplum.spark.GreenplumDataSource") \
                .option("url", greenplum_url) \
                .option("user", greenplum_user) \
                .option("password", greenplum_password) \
                .option("table", external_table_name) \
                .mode("overwrite") \
                .save()
            print(f"Successfully created external table {external_table_name}")
        else:
            casted_df.repartition(20).write \
                .format("io.pivotal.greenplum.spark.GreenplumRelationProvider") \
                .option("url", greenplum_url) \
                .option("user", greenplum_user) \
                .option("password", greenplum_password) \
                .option("table", greenplum_table_name) \
                .mode("overwrite") \
                .save()
            print(f"Successfully wrote data to {greenplum_table_name}")

        greenplum_count = spark.read.format("io.pivotal.greenplum.spark.GreenplumRelationProvider") \
            .option("url", greenplum_url) \
            .option("user", greenplum_user) \
            .option("password", greenplum_password) \
            .option("table", greenplum_table_name) \
            .load() \
            .count()

        hive_count = hive_table_df.count()

        if greenplum_count == hive_count:
            print("Data integrity check passed")
        else:
            print("Data integrity check failed")

    except Exception as e:
        print(f"An error occurred during the migration: {str(e)}")
        raise e

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
