package com.datahack.bootcamp.spark.sql

import org.apache.spark.sql.SparkSession

object UdfExample {

  def main(args: Array[String]) {

    val spark: SparkSession = ???

    firstExample(spark)
    secondExample(spark)
    thirdExample(spark)
    fourthExample(spark)
    fifthExample(spark)
    sixthExample(spark)
    seventhExample(spark)
  }

  /*
   * User Defined Functions
   * Spark SQL has language integrated User-Defined Functions (UDFs).
   * UDF is a feature of Spark SQL to define new Column-based functions that extend the vocabulary of Spark SQL’s DSL
   * for transforming Datasets. UDFs are black boxes in their execution.
   * The example below defines a UDF to convert a given text to upper case.
   */
  def firstExample(spark: SparkSession): Unit = ???

  /*
   * Querying Using Spark SQL
   * We will now start querying using Spark SQL.
   * Note that the actual SQL queries are similar to the ones used in popular SQL clients.
   */
  def secondExample(spark: SparkSession) = ???

  /*
   * Creating Datasets
   * After understanding DataFrames, let us now move on to Dataset API.
   * The below code creates a Dataset class in SparkSQL.
   */
  def thirdExample(spark: SparkSession) = ???

  /*
   * Adding Schema To RDDs
   * Spark introduces the concept of an RDD (Resilient Distributed Dataset), an immutable fault-tolerant,
   * distributed collection of objects that can be operated on in parallel. An RDD can contain any type of object
   * and is created by loading an external dataset or distributing a collection from the driver program.
   * Schema RDD is a RDD where you can run SQL on. It is more than SQL. It is a unified interface for structured data.
   */
  def fourthExample(spark: SparkSession) = ???

  /*
   * RDDs As Relations
   * Resilient Distributed Datasets (RDDs) are distributed memory abstraction which lets programmers perform
   * in-memory computations on large clusters in a fault tolerant manner. RDDs can be created from any data source.
   * Eg: Scala collection, local file system, Hadoop, Amazon S3, HBase Table, etc.
   */
  def fifthExample(spark: SparkSession): Unit = ???

  /*
   * Caching Tables In-Memory
   * Spark SQL caches tables using an in-memory columnar format:
   *    - Scan only required columns
   *    - Fewer allocated objects
   *    - Automatically selects best comparison
   */

  /*
   * Loading Data Programmatically
   * The below code will read employee.json file and create a DataFrame. We will then use it to create a Parquet file.
   */
  def sixthExample(spark: SparkSession): Unit = ???

  /*
   * JSON Datasets
   * We will now work on JSON data. As Spark SQL supports JSON dataset, we create a DataFrame of employee.json.
   * The schema of this DataFrame can be seen below. We then define a Youngster DataFrame and add all the employees
   * between the ages of 18 and 30.
   */
  def seventhExample(spark: SparkSession): Unit = ???

}

case class Employee(name: String, age: Long)