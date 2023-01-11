# spark_hive_greenplum
# HiveToGreenplum

HiveToGreenplum is a Spark application that reads a Hive table in ORC format and uploads it to a Greenplum database.

## Prerequisites

- Apache Spark
- Hive
- Greenplum

## Usage

To run the application, use the following command:

```
spark-submit --class HiveToGreenplum <path-to-jar> <hive-table-name> <greenplum-table-name> <greenplum-schema> <greenplum-url> <greenplum-user> <greenplum-password>
```

## License

This project is licensed under the MIT License.
