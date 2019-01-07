package com.datahack.bootcamp.spark.baseRDD

import org.apache.spark.{SparkConf, SparkContext}

object Cartesian {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    firstExample(sc)

    sc.stop()
  }

  // Este método pinta el contenido de cada partición de un RDD.
  def myfunc(index: Int, iter: Iterator[Any]) : Iterator[Any] = {
    iter.toList.map(x => "[partID: " +  index + ", val: " + x + "]").iterator
  }

  def firstExample(sc: SparkContext): Unit = {
    val x = sc.parallelize(List(1,2,3,4,5))
    val y = sc.parallelize(List(6,7,8,9,10))
    val result = x.cartesian(y).collect

    val xPartitions = x.mapPartitionsWithIndex(myfunc).collect()
    val yPartitions = y.mapPartitionsWithIndex(myfunc).collect()

    println("---------- Example 1 ----------")
    println("-- X: ")
    xPartitions.foreach(println)
    println("-- Y: ")
    yPartitions.foreach(println)
    println("-- Result X cart Y: ")
    result.foreach(println)
  }
}
