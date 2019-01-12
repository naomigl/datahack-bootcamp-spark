package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKey {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("reduce").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    firstExample(sc)
    secondExample(sc)

    sc.stop()
  }

  // Este método pinta el contenido de cada partición de un RDD.
  def myfunc(index: Int, iter: Iterator[Any]) : Iterator[String] = {
    iter.toList.map(x => "[partID: " +  index + ", val: " + x + "]").iterator
  }

  def firstExample(sc: SparkContext): Unit = {
    val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
    val b: RDD[(Int, String)] = a.map(x => (x.length, x))

    val spartition = b.mapPartitionsWithIndex(myfunc).collect()
    val result = b.reduceByKey(_ + _).collect()

    println("-------Example 1-----")
    spartition.foreach(println)
    println("-----Result:")
    result.foreach(println)
  }

  def secondExample(sc: SparkContext): Unit = {
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val b: RDD[(Int, String)] = a.map(x => (x.length, x))

    val spartition = b.mapPartitionsWithIndex(myfunc).collect()
    val result: RDD[(Int, String)] = b.reduceByKey(_ + _)

    val rpartition = result.mapPartitionsWithIndex(myfunc).collect()
    val result2 = result.collect()

    println("-------Example 2-----")
    spartition.foreach(println)
    println("-----Result:")
    result2.foreach(println)
    println()
    rpartition.foreach(println)
  }

}
