package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.{SparkConf, SparkContext}

object ReduceByKey {

  def main(args: Array[String]) {
    val conf: SparkConf = ???
    val sc: SparkContext = ???

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
  }

  def secondExample(sc: SparkContext): Unit = {
    val a = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
  }

}
