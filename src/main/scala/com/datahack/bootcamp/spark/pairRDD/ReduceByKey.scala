package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKey {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    firstExample(sc)
    secondExample(sc)

    sc.stop()
  }

  // lets have a look at what is in the partitions
  def myfunc(index: Int, iter: Iterator[Any]) : Iterator[String] = {
    iter.toList.map(x => "[partID: " +  index + ", val: " + x + "]").iterator
  }

  def firstExample(sc: SparkContext): Unit = {
    val a = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
    val b = a.map(x => (x.length, x))

    val spartitions = b.mapPartitionsWithIndex(myfunc).collect()
    val result = b.reduceByKey(_ + _).collect

    println("---------- Example 1 ----------")
    spartitions.foreach(println)
    println(s"--Result: ")
    result.foreach(println)
    println()
  }

  def secondExample(sc: SparkContext): Unit = {
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val b = a.map(x => (x.length, x))

    val spartitions = b.mapPartitionsWithIndex(myfunc).collect()
    val result = b.reduceByKey(_ + _).collect

    println("---------- Example 2 ----------")
    spartitions.foreach(println)
    println(s"--Result: ")
    result.foreach(println)
    println()
  }

}
