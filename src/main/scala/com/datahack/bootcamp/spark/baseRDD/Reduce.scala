package com.datahack.bootcamp.spark.baseRDD

import org.apache.spark.{SparkConf, SparkContext}

object Reduce {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    firstExample(sc)

    sc.stop()
  }

  // lets first print out the contents of the RDD with partition labels
  def myfunc(index: Int, iter: Iterator[Any]) : Iterator[String] = {
    iter.toList.map(x => "[partID: " +  index + ", val: " + x + "]").iterator
  }

  def firstExample(sc: SparkContext): Unit = {
    val a = sc.parallelize(1 to 100, 3)

    val spartitions = a.mapPartitionsWithIndex(myfunc).collect()
    val result = a.reduce(_ + _)

    println("---------- Example 1 ----------")
    spartitions.foreach(println)
    println(s"--Result: $result")
  }
}
