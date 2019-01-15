package com.datahack.bootcamp.spark.streaming

import java.util.concurrent.Executors

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.streaming.StreamingQuery

object FirstExercise extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Streaming example 1")
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext

  import spark.implicits._

  // Register a StreamingQueryListener to receive notifications about state changes of streaming queries
  import org.apache.spark.sql.streaming.StreamingQueryListener

  val myQueryListener = new StreamingQueryListener {

    import org.apache.spark.sql.streaming.StreamingQueryListener._

    def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
      println(s"Query ${event.id} terminated")
    }

    def onQueryStarted(event: QueryStartedEvent): Unit = {}

    def onQueryProgress(event: QueryProgressEvent): Unit = {
      println(s"Query on progress ${event.progress}")
    }
  }
  spark.streams.addListener(myQueryListener)

  val bidSchema: StructType = new StructType()
    .add("bidid", StringType)
    .add("timestamp", StringType)
    .add("ipinyouid", StringType)
    .add("useragent", StringType)
    .add("IP", StringType)
    .add("region", IntegerType)
    .add("cityID", IntegerType)
    .add("adexchange", StringType)
    .add("domain", StringType)
    .add("turl", StringType)
    .add("urlid", StringType)
    .add("slotid", StringType)
    .add("slotwidth", StringType)
    .add("slotheight", StringType)
    .add("slotvisibility", StringType)
    .add("slotformat", StringType)
    .add("slotprice", StringType)
    .add("creative", StringType)
    .add("bidprice", StringType)

  val streamingInputDF: DataFrame = spark.readStream
    .format("csv")
    .schema(bidSchema)
    .option("header", false)
    .option("inferSchema", true)
    .option("sep", "\t")
    .option("maxFilesPerTrigger", 1)
    .load("file:///Users/rafaelgarrote/Downloads/ipinyou.contest.dataset-season2/training2nd")
  streamingInputDF.printSchema()

  val ts: Column = unix_timestamp($"timestamp", "yyyyMMddHHmmssSSS").cast("timestamp")
  val streamingCityTimeDF: DataFrame = streamingInputDF.withColumn("ts", ts).select($"cityID", $"ts")

  // Número de bids por ciudad en ventanas de tiempo de 10 minutos
  val windowedCounts: StreamingQuery = streamingCityTimeDF
    .groupBy(window($"ts", "2 minutes", "1 minutes"), $"cityID")
    .count()
    .writeStream.outputMode("complete")
    .format("console").start()

  import java.util.concurrent.Executors
  import java.util.concurrent.TimeUnit.SECONDS

  def queryTerminator(query: StreamingQuery) = new Runnable {
    def run = {
      println(s"Stopping streaming query: ${query.id}")
      query.stop
    }
  }

  import java.util.concurrent.TimeUnit.SECONDS

  Executors.newSingleThreadScheduledExecutor.
    scheduleWithFixedDelay(queryTerminator(windowedCounts), 1200, 60 * 20, SECONDS)

  spark.streams.awaitAnyTermination
}

object SecondExercise extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Streaming example 2")
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext

  import spark.implicits._

  // Register a StreamingQueryListener to receive notifications about state changes of streaming queries
  import org.apache.spark.sql.streaming.StreamingQueryListener

  val myQueryListener = new StreamingQueryListener {

    import org.apache.spark.sql.streaming.StreamingQueryListener._

    def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
      println(s"Query ${event.id} terminated")
    }

    def onQueryStarted(event: QueryStartedEvent): Unit = {}

    def onQueryProgress(event: QueryProgressEvent): Unit = {
      println(s"Query on progress ${event.progress}")
    }
  }
  spark.streams.addListener(myQueryListener)

  val bidSchema: StructType = new StructType()
    .add("bidid", StringType)
    .add("timestamp", StringType)
    .add("ipinyouid", StringType)
    .add("useragent", StringType)
    .add("IP", StringType)
    .add("region", IntegerType)
    .add("cityID", IntegerType)
    .add("adexchange", StringType)
    .add("domain", StringType)
    .add("turl", StringType)
    .add("urlid", StringType)
    .add("slotid", StringType)
    .add("slotwidth", StringType)
    .add("slotheight", StringType)
    .add("slotvisibility", StringType)
    .add("slotformat", StringType)
    .add("slotprice", StringType)
    .add("creative", StringType)
    .add("bidprice", StringType)

  val streamingInputDF: DataFrame = spark.readStream
    .format("csv")
    .schema(bidSchema)
    .option("header", false)
    .option("inferSchema", true)
    .option("sep", "\t")
    .option("maxFilesPerTrigger", 1)
    .load("file:///Users/rafaelgarrote/Downloads/ipinyou.contest.dataset-season2/training2nd")
  streamingInputDF.printSchema()

  val ts: Column = unix_timestamp($"timestamp", "yyyyMMddHHmmssSSS").cast("timestamp")
  val streamingCityTimeDF: DataFrame = streamingInputDF.withColumn("ts", ts).select($"cityID", $"ts")

  //Join de dos dataframes en streaming
  val citySchema = new StructType().add("cityID", StringType).add("cityName", StringType)
  val staticDF: DataFrame = spark.read
    .format("csv")
    .schema(citySchema)
    .option("header", false)
    .option("inferSchema", true)
    .option("sep", "\t")
    .load("file:///Users/rafaelgarrote/Downloads/ipinyou.contest.dataset-season2/city.en.txt")
  val joinedDF = streamingCityTimeDF.join(staticDF, "cityID")

  val windowedCityCounts: StreamingQuery = joinedDF
    .groupBy(window($"ts", "10 minutes", "5 minutes"), $"cityName")
    .count()
    .writeStream.outputMode("complete")
    .format("console").start()

  def queryTerminator(query: StreamingQuery) = new Runnable {
    def run = {
      println(s"Stopping streaming query: ${query.id}")
      query.stop
    }
  }

  import java.util.concurrent.TimeUnit.SECONDS

  Executors.newSingleThreadScheduledExecutor.
    scheduleWithFixedDelay(queryTerminator(windowedCityCounts), 1200, 60 * 5, SECONDS)

  spark.streams.awaitAnyTermination
}

object ThirdExercise extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Streaming example 3")
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext

  import spark.implicits._

  // Register a StreamingQueryListener to receive notifications about state changes of streaming queries
  import org.apache.spark.sql.streaming.StreamingQueryListener

  val myQueryListener = new StreamingQueryListener {

    import org.apache.spark.sql.streaming.StreamingQueryListener._

    def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
      println(s"Query ${event.id} terminated")
    }

    def onQueryStarted(event: QueryStartedEvent): Unit = {}

    def onQueryProgress(event: QueryProgressEvent): Unit = {
      println(s"Query on progress ${event.progress}")
    }
  }
  spark.streams.addListener(myQueryListener)

  val bidSchema: StructType = new StructType()
    .add("bidid", StringType)
    .add("timestamp", StringType)
    .add("ipinyouid", StringType)
    .add("useragent", StringType)
    .add("IP", StringType)
    .add("region", IntegerType)
    .add("cityID", IntegerType)
    .add("adexchange", StringType)
    .add("domain", StringType)
    .add("turl", StringType)
    .add("urlid", StringType)
    .add("slotid", StringType)
    .add("slotwidth", StringType)
    .add("slotheight", StringType)
    .add("slotvisibility", StringType)
    .add("slotformat", StringType)
    .add("slotprice", StringType)
    .add("creative", StringType)
    .add("bidprice", StringType)

  val streamingInputDF: DataFrame = spark.readStream
    .format("csv")
    .schema(bidSchema)
    .option("header", false)
    .option("inferSchema", true)
    .option("sep", "\t")
    .option("maxFilesPerTrigger", 1)
    .load("file:///Users/rafaelgarrote/Downloads/ipinyou.contest.dataset-season2/training2nd")
  streamingInputDF.printSchema()

  val citySchema = new StructType().add("cityID", StringType).add("cityName", StringType)
  val staticDF: DataFrame = spark.read
    .format("csv")
    .schema(citySchema)
    .option("header", false)
    .option("inferSchema", true)
    .option("sep", "\t")
    .load("file:///Users/rafaelgarrote/Downloads/ipinyou.contest.dataset-season2/city.en.txt")

  val ts: Column = unix_timestamp($"timestamp", "yyyyMMddHHmmssSSS").cast("timestamp")
  val streamingCityNameBidsTimeDF = streamingInputDF.
    withColumn("ts", ts)
    .select($"ts", $"bidid", $"cityID", $"bidprice", $"slotprice")
    .join(staticDF, "cityID")

  val cityBids: StreamingQuery = streamingCityNameBidsTimeDF
    .select($"ts", $"bidid", $"bidprice", $"slotprice", $"cityName")
    .writeStream.outputMode("append")
    .format("console")
    .start()

  def queryTerminator(query: StreamingQuery) = new Runnable {
    def run = {
      println(s"Stopping streaming query: ${query.id}")
      query.stop
    }
  }

  import java.util.concurrent.TimeUnit.SECONDS

  Executors.newSingleThreadScheduledExecutor.
    scheduleWithFixedDelay(queryTerminator(cityBids), 1200, 60 * 5, SECONDS)

  spark.streams.awaitAnyTermination

}

object FourthExample extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Streaming example 2")
    .master("local[2]")
    .enableHiveSupport()
    .getOrCreate()
  val sc: SparkContext = spark.sparkContext

  import spark.implicits._

  // Register a StreamingQueryListener to receive notifications about state changes of streaming queries
  import org.apache.spark.sql.streaming.StreamingQueryListener

  val myQueryListener = new StreamingQueryListener {

    import org.apache.spark.sql.streaming.StreamingQueryListener._

    def onQueryTerminated(event: QueryTerminatedEvent): Unit = {
      println(s"Query ${event.id} terminated")
    }

    def onQueryStarted(event: QueryStartedEvent): Unit = {}

    def onQueryProgress(event: QueryProgressEvent): Unit = {
      println(s"Query on progress ${event.progress}")
    }
  }
  spark.streams.addListener(myQueryListener)

  val bidSchema: StructType = new StructType()
    .add("bidid", StringType)
    .add("timestamp", StringType)
    .add("ipinyouid", StringType)
    .add("useragent", StringType)
    .add("IP", StringType)
    .add("region", IntegerType)
    .add("cityID", IntegerType)
    .add("adexchange", StringType)
    .add("domain", StringType)
    .add("turl", StringType)
    .add("urlid", StringType)
    .add("slotid", StringType)
    .add("slotwidth", StringType)
    .add("slotheight", StringType)
    .add("slotvisibility", StringType)
    .add("slotformat", StringType)
    .add("slotprice", StringType)
    .add("creative", StringType)
    .add("bidprice", StringType)

  val streamingInputDF: DataFrame = spark.readStream
    .format("csv")
    .schema(bidSchema)
    .option("header", false)
    .option("inferSchema", true)
    .option("sep", "\t")
    .option("maxFilesPerTrigger", 1)
    .load("file:///Users/rafaelgarrote/Downloads/ipinyou.contest.dataset-season2/training2nd")
  streamingInputDF.printSchema()

  //Code for Using the Dataset API in Structured Streaming section
  case class Bid(
                  bidid: String,
                  timestamp: String,
                  ipinyouid: String,
                  useragent: String,
                  IP: String,
                  region: Integer,
                  cityID: Integer,
                  adexchange: String,
                  domain: String,
                  turl: String,
                  urlid: String,
                  slotid: String,
                  slotwidth: String,
                  slotheight: String,
                  slotvisibility: String,
                  slotformat: String,
                  slotprice: String,
                  creative: String,
                  bidprice: String)

  val ds: Dataset[Bid] = streamingInputDF.as[Bid]

  //Code for Using the Foreach Sink for arbitrary computations on output section
  import org.apache.spark.sql.ForeachWriter

  val writer = new ForeachWriter[String] {
    override def open(partitionId: Long, version: Long) = true
    override def process(value: String) = println(value)
    override def close(errorOrNull: Throwable) = {}
  }

  val dsForeach: StreamingQuery = ds.filter(_.adexchange == "3").map(_.useragent).writeStream.foreach(writer).start()

  //Code for Using the Memory Sink to save output to a table section
  val aggAdexchangeDF = streamingInputDF.groupBy($"adexchange").count()

  //Wait for the output show on the screen after the next statement
  val aggQuery = aggAdexchangeDF
    .writeStream.queryName("aggregateTable")
    .outputMode("complete")
    .format("memory")
    .start()

  spark.sql("select * from aggregateTable").show()

  val citySchema = new StructType().add("cityID", StringType).add("cityName", StringType)
  val staticDF: DataFrame = spark.read
    .format("csv")
    .schema(citySchema)
    .option("header", false)
    .option("inferSchema", true)
    .option("sep", "\t")
    .load("file:///Users/rafaelgarrote/Downloads/ipinyou.contest.dataset-season2/city.en.txt")

  val ts: Column = unix_timestamp($"timestamp", "yyyyMMddHHmmssSSS").cast("timestamp")
  val streamingCityNameBidsTimeDF = streamingInputDF.
    withColumn("ts", ts)
    .select($"ts", $"bidid", $"cityID", $"bidprice", $"slotprice")
    .join(staticDF, "cityID")

  //Code for Using the File Sink to save output to a partitioned table section
  val cityBidsParquet = streamingCityNameBidsTimeDF
    .select($"bidid", $"bidprice", $"slotprice", $"cityName")
    .writeStream.outputMode("append")
    .format("parquet")
    .option("path", "hdfs://localhost:9000/pout")
    .option("checkpointLocation", "hdfs://localhost:9000/poutcp")
    .start()

  def queryTerminator(query: StreamingQuery) = new Runnable {
    def run = {
      println(s"Stopping streaming query: ${query.id}")
      query.stop
    }
  }

  import java.util.concurrent.TimeUnit.SECONDS

  Executors.newSingleThreadScheduledExecutor
    .scheduleWithFixedDelay(queryTerminator(cityBidsParquet), 1200, 60 * 5, SECONDS)

  spark.streams.awaitAnyTermination

}
