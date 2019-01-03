package com.datahack.bootcamp.spark.sql

import org.apache.spark.sql.SparkSession

object UdfExample {

  def main(args: Array[String]) {

    val spark: SparkSession = SparkSession
      .builder()
      .appName("UDF Example")
      .getOrCreate()

    firstExample(spark)
    secondExample(spark)
    thirdExample(spark)

  }

  /*
   * User Defined Functions
   * Spark SQL has language integrated User-Defined Functions (UDFs).
   * UDF is a feature of Spark SQL to define new Column-based functions that extend the vocabulary of Spark SQL’s DSL
   * for transforming Datasets. UDFs are black boxes in their execution.
   * The example below defines a UDF to convert a given text to upper case.
   */
  def firstExample(spark: SparkSession): Unit = {

    import spark.implicits._

    val dataset = Seq((0, "hello"),(1, "world")).toDF("id","text")
    val upper: String => String = _.toUpperCase

    import org.apache.spark.sql.functions.udf
    val upperUDF = udf(upper)
    dataset.withColumn("upper", upperUDF('text)).show

    spark.udf.register("myUpper", (input: String) => input.toUpperCase)

    spark.catalog.listFunctions.filter('name like "%upper%").show(false)
  }

  /*
   * Querying Using Spark SQL
   * We will now start querying using Spark SQL.
   * Note that the actual SQL queries are similar to the ones used in popular SQL clients.
   */
  def secondExample(spark: SparkSession) = {

    import spark.implicits._

    val df = spark.read.json("examples/src/main/resources/employee.json")
    df.show()

    df.printSchema()
    df.select("name").show()

    df.select($"name", $"age" + 2).show()
    df.filter($"age" > 30).show()
    df.groupBy("age").count().show()

    df.createOrReplaceTempView("employee")

    val sqlDF = spark.sql("SELECT * FROM employee")
    sqlDF.show()

  }

  /*
   * Creating Datasets
   * After understanding DataFrames, let us now move on to Dataset API.
   * The below code creates a Dataset class in SparkSQL.
   */
  def thirdExample(spark: SparkSession) = {
    import spark.implicits._

    val caseClassDS = Seq(Employee("Andrew", 55)).toDS()
    caseClassDS.show()

    val path = "examples/src/main/resources/employee.json"
    val employeeDS = spark.read.json(path).as[Employee]
    employeeDS.show()
  }

  /*
   * Adding Schema To RDDs
   * Spark introduces the concept of an RDD (Resilient Distributed Dataset), an immutable fault-tolerant,
   * distributed collection of objects that can be operated on in parallel. An RDD can contain any type of object
   * and is created by loading an external dataset or distributing a collection from the driver program.
   * Schema RDD is a RDD where you can run SQL on. It is more than SQL. It is a unified interface for structured data.
   */
  def fourthExample(spark: SparkSession) = {
    import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
    import org.apache.spark.sql.Encoder
    import spark.implicits._
    val employeeDF = spark.sparkContext.textFile("examples/src/main/resources/employee.txt")
      .map(_.split(","))
      .map(attributes => Employee(attributes(0), attributes(1).trim.toInt)).toDF()
    employeeDF.createOrReplaceTempView("employee")
    val youngstersDF = spark.sql("SELECT name, age FROM employee WHERE age BETWEEN 18 AND 30")
    youngstersDF.map(youngster => "Name: " + youngster(0)).show()

    youngstersDF.map(youngster => "Name: " + youngster.getAs[String]("name")).show()
    implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, Any]]
    youngstersDF.map(youngster => youngster.getValuesMap[Any](List("name", "age"))).collect()
  }

  /*
   * RDDs As Relations
   * Resilient Distributed Datasets (RDDs) are distributed memory abstraction which lets programmers perform
   * in-memory computations on large clusters in a fault tolerant manner. RDDs can be created from any data source.
   * Eg: Scala collection, local file system, Hadoop, Amazon S3, HBase Table, etc.
   */
  /*def fifthExample(spark: SparkSession): Unit = {
    import org.apache.spark.sql.types._
    import org.apache.spark.sql.Row
    val employeeRDD = spark.sparkContext.textFile("examples/src/main/resources/employee.txt")
    val schemaString = "name age"
    val fields = schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, nullable = true))
    val schema = StructType(fields)

    val rowRDD = employeeRDD.map(_.split(",")).map(attributes => Row(attributes(0), attributes(1).trim))
    val employeeDF = spark.createDataFrame(rowRDD, schema)
    employeeDF.createOrReplaceTempView("employee")
    val results = spark.sql("SELECT name FROM employee")
    results.map(attributes => "Name: " + attributes(0)).show()
  }*/

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
  def sixthExample(spark: SparkSession) = {
    import spark.implicits._
    val employeeDF = spark.read.json("examples/src/main/resources/employee.json")
    employeeDF.write.parquet("employee.parquet")
    val parquetFileDF = spark.read.parquet("employee.parquet")
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 18 AND 30")
    namesDF.map(attributes => "Name: " + attributes(0)).show()
  }

  /*
   * JSON Datasets
   * We will now work on JSON data. As Spark SQL supports JSON dataset, we create a DataFrame of employee.json.
   * The schema of this DataFrame can be seen below. We then define a Youngster DataFrame and add all the employees
   * between the ages of 18 and 30.
   */
  def seventhExample(spark: SparkSession): Unit = {
    val path = "examples/src/main/resources/employee.json"
    val employeeDF = spark.read.json(path)
    employeeDF.printSchema()
    employeeDF.createOrReplaceTempView("employee")
    val youngsterNamesDF = spark.sql("SELECT name FROM employee WHERE age BETWEEN 18 AND 30")
    youngsterNamesDF.show()

    val otherEmployeeRDD = spark.sparkContext
      .makeRDD("""{"name":"George","address":{"city":"New Delhi","state":"Delhi"}}""" :: Nil)
    val otherEmployee = spark.read.json(otherEmployeeRDD)
    otherEmployee.show()
  }

  /*
   * Hive Tables
   * We perform a Spark example using Hive tables.
   */
  /*def eightExample(spak: SparkSession): Unit = {
    import org.apache.spark.sql.Row
    import org.apache.spark.sql.SparkSession
    case class Record(key: Int, value: String)
    val warehouseLocation = "spark-warehouse"
    val spark = SparkSession.builder().appName("Spark Hive Example").
      config("spark.sql.warehouse.dir", warehouseLocation).enableHiveSupport().getOrCreate()
    import spark.implicits._
    import spark.sql
    sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")

    sql("LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src")
    sql("SELECT * FROM src").show()

    sql("SELECT COUNT(*) FROM src").show()
    val sqlDF = sql("SELECT key, value FROM src WHERE key &amp;amp;amp;amp;lt; 10 ORDER BY key")
    val stringsDS = sqlDF.map {case Row(key: Int, value: String) => s"Key: $key, Value: $value"}
    stringsDS.show()

    val recordsDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
    recordsDF.createOrReplaceTempView("records")
    sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()

  }*/

}

case class Employee(name: String, age: Long)