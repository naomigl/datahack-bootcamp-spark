package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object PerKeyAverage {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    val pairRDD: RDD[(String, Int)] =
      sc.parallelize(List(("cat", 2), ("cat", 5), ("mouse", 4), ("cat", 12), ("dog", 12), ("mouse", 2)), 2)

    val spartitions: Array[String] = pairRDD.mapPartitionsWithIndex(myfunc).collect()
    val averageAggregated: Array[(String, Float)] = getPerKeyAverageUsingAggregateByKey(pairRDD).collect()
    val averageCombined = getPerKeyAverageUsingCombineByKey(pairRDD).collect()
    val averageReduced = getPerKeyAverageUsingReduceByKey(pairRDD).collect()

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

  // lets have a look at what is in the partitions
  def myfunc(index: Int, iter: Iterator[(String, Int)]) : Iterator[String] = {
    iter.toList.map(x => "[partID: " +  index + ", val: " + x + "]").iterator
  }

  def getPerKeyAverageUsingAggregateByKey(values: RDD[(String, Int)]): RDD[(String, Float)] = {
    val zeroValue = (0,0)
    val mergeValue = (v1: (Int, Int), v2: Int) => (v1._1 + v2, v1._2 + 1)
    val mergeCombiners = (v1: (Int, Int), v2: (Int, Int)) => (v1._1 + v2._1, v1._2 + v2._2)

    values.aggregateByKey(zeroValue) (mergeValue, mergeCombiners)
      .map(f => (f._1, f._2._1.toFloat / f._2._2.toFloat))
  }

  def getPerKeyAverageUsingCombineByKey(values: RDD[(String, Int)]): RDD[(String, Float)] = {
    val createCombiner = (v: Int) => (v, 1)
    val mergeValue = (v1: (Int, Int), v2: Int) => (v1._1 + v2, v1._2 + 1)
    val mergeCombiners = (v1: (Int, Int), v2: (Int, Int)) => (v1._1 + v2._1, v1._2 + v2._2)

    values.combineByKey(
      createCombiner,
      mergeValue,
      mergeCombiners)
      .map(f => (f._1, f._2._1.toFloat / f._2._2.toFloat))
  }


  def getPerKeyAverageUsingReduceByKey(values: RDD[(String, Int)]): RDD[(String, Float)] = {
    values
      .mapValues(v => (v,1))
      .reduceByKey((v1,v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .map(f => (f._1, f._2._1.toFloat / f._2._2.toFloat))
  }

}
