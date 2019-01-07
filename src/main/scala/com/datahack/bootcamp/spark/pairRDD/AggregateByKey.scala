package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object AggregateByKey {

  def main(args: Array[String]) {
    val conf: SparkConf = ???
    val sc: SparkContext = ???

    firstExample(sc)

    sc.stop()
  }

  // Este método pinta el contenido de cada partición de un RDD.
  def myfunc(index: Int, iter: Iterator[(String, Int)]) : Iterator[String] = {
    iter.toList.map(x => "[partID: " +  index + ", val: " + x + "]").iterator
  }

  def firstExample(sc: SparkContext): Unit = {
    val pairRDD: RDD[(String, Int)] =
      sc.parallelize(List( ("cat",2), ("cat", 5), ("mouse", 4),("cat", 12), ("dog", 12), ("mouse", 2)), 2)

  }

}
