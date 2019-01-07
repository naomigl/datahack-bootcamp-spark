package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FoldByKey {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    firstExample(sc)
    secondExample(sc)

    sc.stop()
  }

  def firstExample(sc: SparkContext): Unit = {
    val rddA: RDD[String] = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
    val rddB: RDD[(Int, String)] = rddA.map(x => (x.length, x))
    val result: Array[(Int, String)] = rddB.foldByKey("")(_ + _).collect

    println("---------- Example 1 ----------")
    println(s"-- Result:")
    result.foreach(println)
  }

  def secondExample(sc: SparkContext): Unit = {
    val rddA: RDD[String] = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
    val rddB: RDD[(Int, String)] = rddA.map(x => (x.length, x))
    val result: Array[(Int, String)] = rddB.foldByKey("")(_ + _).collect

    println("---------- Example 2 ----------")
    println(s"-- Result:")
    result.foreach(println)
  }
}
