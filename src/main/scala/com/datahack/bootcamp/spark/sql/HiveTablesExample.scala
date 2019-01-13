package com.datahack.bootcamp.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession


object HiveTablesExample extends App {

  val warehouseLocation = "spark-warehouse"
  val spark = SparkSession.builder()
    .appName("Spark Hive Example")
    .master("local[2]")
    .config("spark.sql.warehouse.dir", warehouseLocation)
    .enableHiveSupport()
    .getOrCreate()

  performExample

  /*
   * Hive Tables
   * We perform a Spark example using Hive tables.
   */
  def performExample: Unit = {

    import spark.implicits._
    import spark.sql

    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")

    sql("LOAD DATA LOCAL INPATH 'src/main/resources/kv1.txt' INTO TABLE src")
    sql("SELECT * FROM src").show()

    sql("SELECT COUNT(*) FROM src").show()
    val sqlDF = sql("SELECT key, value FROM src WHERE key > 10 ORDER BY key")
    val stringsDS = sqlDF.map {case Row(key: Int, value: String) => s"Key: $key, Value: $value"}
    stringsDS.show()

    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()

  }

  case class Record(key: Int, value: String)
}
