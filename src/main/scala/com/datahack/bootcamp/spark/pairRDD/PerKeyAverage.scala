package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PerKeyAverage {

  def main(args: Array[String]) {
    val conf: SparkConf = ???
    val sc: SparkContext = ???

    val pairRDD: RDD[(String, Int)] =
      sc.parallelize(List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 12), ("dog", 12), ("mouse", 2)), 2)

    val spartitions: Array[String] = ???
    val averageAggregated: Array[(String, Float)] = ???
    val averageCombined: Array[(String, Float)] = ???
    val averageReduced: Array[(String, Float)] = ???

    println("------ Values: ")
    spartitions.foreach(println)
    println("------ Average Aggregated: ")
    averageAggregated.foreach(println)
    println("------ Average Combined: ")
    averageCombined.foreach(println)
    println("------ Average Reduced: ")
    averageReduced.foreach(println)
    sc.stop()
  }

  // Este método pinta el contenido de cada partición de un RDD.
  def myfunc(index: Int, iter: Iterator[(String, Int)]) : Iterator[String] = {
    iter.toList.map(x => "[partID: " +  index + ", val: " + x + "]").iterator
  }

  // TODO: Obten la media por clave utilizando aggregateByKey
  def getPerKeyAverageUsingAggregateByKey(values: RDD[(String, Int)]): RDD[(String, Float)] = ???

  // TODO: Obten la media por clave utilizando combineByKey
  def getPerKeyAverageUsingCombineByKey(values: RDD[(String, Int)]): RDD[(String, Float)] = ???

  // TODO: Obten la media por clave utilizando reduceByKey
  def getPerKeyAverageUsingReduceByKey(values: RDD[(String, Int)]): RDD[(String, Float)] = ???

}
