import org.apache.spark.sql.types._
import org.scalatest.funsuite.AnyFunSuite

class HiveToGreenplumTest extends AnyFunSuite {

  test("schemaToSql") {
    val schema = StructType(Seq(
      StructField("column1", IntegerType),
      StructField("column2", StringType),
      StructField("column3", DateType)
    ))
    val tableName = "test_table"

    val result = HiveToGreenplum.schemaToSql(schema, tableName)

    val expected = "CREATE TABLE test_table (column1 INT, column2 TEXT, column3 DATE)"
    assert(result == expected)
  }
}
