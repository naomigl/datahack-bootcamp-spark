package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FoldByKey {

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
    val rddA: RDD[String] = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
    val rddb: RDD[(Int, String)] = rddA.map(x => (x.length, x))
    val spartition = rddb.mapPartitionsWithIndex(myfunc)

    val result = rddb.foldByKey("")(_ + _)
    println("-------Example 1-----")
    spartition.foreach(println)
    println("-----Result:")
    result.foreach(println)
  }

  def secondExample(sc: SparkContext): Unit = {
    val rddA: RDD[String] = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val rddb: RDD[(Int, String)] = rddA.map(x => (x.length, x))
    val spartition = rddb.mapPartitionsWithIndex(myfunc)

    val result = rddb.foldByKey("")(_ + _)
    println("-------Example 1-----")
    spartition.foreach(println)
    println("-----Result:")
    result.foreach(println)
  }
}
