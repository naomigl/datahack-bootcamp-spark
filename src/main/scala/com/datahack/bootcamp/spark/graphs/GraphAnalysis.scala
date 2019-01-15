package com.datahack.bootcamp.spark.graphs

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD


// https://www.fordgobike.com/system-data
object GraphAnalysis extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Graph Analysis")
    .master("local[2]")
    .getOrCreate()

  import spark.implicits._

  // Importamos los datos
  val bikeStationsDF: DataFrame = spark.read.json("src/main/resources/stations.json")
  val tripDataDF: DataFrame = spark.read.csv("src/main/resources/201812-fordgobike-tripdata.csv")

  bikeStationsDF.createOrReplaceTempView("sf_201812_station_data")
  tripDataDF.createOrReplaceTempView("sf_201812_trip_data")

  // Creamos los dataframnes que necesitamos para crear el grafo
  val bikeStations = spark.sql("SELECT * FROM sf_201812_station_data  ")
  val tripData = spark.sql("SELECT * FROM sf_201812_trip_data")

  bikeStations.printSchema()
  bikeStations.show()

  tripData.printSchema()
  tripData.show()

  // Creamos el grafo
  val justStations = bikeStations
    .selectExpr("float(station_id) as station_id", "name")
    .distinct()

  val completeTripData = tripData
    .join(justStations, tripData("Start Station") === bikeStations("name"))
    .withColumnRenamed("station_id", "start_station_id")
    .drop("name")
    .join(justStations, tripData("End Station") === bikeStations("name"))
    .withColumnRenamed("station_id", "end_station_id")
    .drop("name")

  val stations = completeTripData
    .select("start_station_id", "end_station_id")
    .rdd
    .distinct() // helps filter out duplicate trips
    .flatMap(x => Iterable(x(0).asInstanceOf[Number].longValue, x(1).asInstanceOf[Number].longValue)) // helps us maintain types
    .distinct()
    .toDF() // return to a DF to make merging + joining easier

  println(stations.take(1)) // this is just a station_id at this point

  // Creamos los vértices
  val stationVertices: RDD[(VertexId, String)] = stations
    .join(justStations, stations("value") === justStations("station_id"))
    .select("station_id", "name")
    .rdd
    .map(row => (row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[String])) // maintain type information

  println(stationVertices.take(1))

  // Creamos los nodos
  val stationEdges:RDD[Edge[Long]] = completeTripData
    .select("start_station_id", "end_station_id")
    .rdd
    .map(row => Edge(row(0).asInstanceOf[Number].longValue, row(1).asInstanceOf[Number].longValue, 1))

  // Creamos el grafo
  val defaultStation = ("Missing Station")
  val stationGraph = Graph(stationVertices, stationEdges, defaultStation)
  stationGraph.cache()

  println("Total Number of Stations: " + stationGraph.numVertices)
  println("Total Number of Trips: " + stationGraph.numEdges)
  // sanity check
  println("Total Number of Trips in Original Data: " + tripData.count)


  // Vamos a aplicar algunos algoritmos de grafos sobre el grafo que hemos construido

  // PageRank
  val ranks = stationGraph.pageRank(0.0001).vertices
  ranks
    .join(stationVertices)
    .sortBy(_._2._1, ascending=false) // sort by the rank
    .take(10) // get the top 10
    .foreach(x => println(x._2._2))

  // Viajes de estación a estación
  stationGraph
    .groupEdges((edge1, edge2) => edge1 + edge2)
    .triplets
    .sortBy(_.attr, ascending=false)
    .map(triplet =>
      "There were " + triplet.attr.toString + " trips from " + triplet.srcAttr + " to " + triplet.dstAttr + ".")
    .take(10)
    .foreach(println)

  // In Degrees
  stationGraph
    .inDegrees // computes in Degrees
    .join(stationVertices)
    .sortBy(_._2._1, ascending=false)
    .take(10)
    .foreach(x => println(x._2._2 + " has " + x._2._1 + " in degrees."))

  // Out Degrees
  stationGraph
    .outDegrees // out degrees
    .join(stationVertices)
    .sortBy(_._2._1, ascending=false)
    .take(10)
    .foreach(x => println(x._2._2 + " has " + x._2._1 + " out degrees."))

  // In Degree / Out Degree ratios
  stationGraph
    .inDegrees
    .join(stationGraph.outDegrees) // join with out Degrees
    .join(stationVertices) // join with our other stations
    .map(x => (x._2._1._1.toDouble/x._2._1._2.toDouble, x._2._2)) // ratio of in to out
    .sortBy(_._1, ascending=false)
    .take(5)
    .foreach(x => println(x._2 + " has a in/out degree ratio of " + x._1))

  stationGraph
    .inDegrees
    .join(stationGraph.inDegrees) // join with out Degrees
    .join(stationVertices) // join with our other stations
    .map(x => (x._2._1._1.toDouble/x._2._1._2.toDouble, x._2._2)) // ratio of in to out
    .sortBy(_._1)
    .take(5)
    .foreach(x => println(x._2 + " has a in/out degree ratio of " + x._1))

}
