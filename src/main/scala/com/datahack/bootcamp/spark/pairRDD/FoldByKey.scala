package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FoldByKey {

  def main(args: Array[String]) {
    val conf: SparkConf = ???
    val sc: SparkContext = ???

    firstExample(sc)
    secondExample(sc)

    sc.stop()
  }

  def firstExample(sc: SparkContext): Unit = {
    val rddA: RDD[String] = sc.parallelize(List("dog", "cat", "owl", "gnu", "ant"), 2)
  }

  def secondExample(sc: SparkContext): Unit = {
    val rddA: RDD[String] = sc.parallelize(List("dog", "tiger", "lion", "cat", "panther", "eagle"), 2)
  }
}
