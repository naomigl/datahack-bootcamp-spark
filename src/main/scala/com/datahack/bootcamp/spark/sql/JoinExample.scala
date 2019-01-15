package com.datahack.bootcamp.spark.sql

import org.apache.spark.sql.SparkSession

object JoinExample extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Join example")
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()

  import spark.implicits._

  val left = Seq((0, "zero"), (1, "one")).toDF("id", "left")
  val right = Seq((0, "zero"), (2, "two"), (3, "three")).toDF("id", "right")

}
