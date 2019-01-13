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

  // Inner join
  left.join(right, "id").show
  left.join(right, "id").explain

  // Full outer
  left.join(right, Seq("id"), "fullouter").show
  left.join(right, Seq("id"), "fullouter").explain

  // Left anti
  left.join(right, Seq("id"), "leftanti").show
  left.join(right, Seq("id"), "leftanti").explain

  case class Person(id: Long, name: String, cityId: Long)
  case class City(id: Long, name: String)
  val family = Seq(
    Person(0, "Agata", 0),
    Person(1, "Iweta", 0),
    Person(2, "Patryk", 2),
    Person(3, "Maksym", 0)).toDS
  val cities = Seq(
    City(0, "Warsaw"),
    City(1, "Washington"),
    City(2, "Sopot")).toDS

  val joined = family.joinWith(cities, family("cityId") === cities("id"))
  joined.printSchema
  joined.show
}
