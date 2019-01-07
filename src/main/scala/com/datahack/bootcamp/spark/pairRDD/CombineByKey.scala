package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.{SparkConf, SparkContext}

object CombineByKey {

  def main(args: Array[String]) {
    val conf: SparkConf = ???
    val sc: SparkContext = ???

    firstExample(sc)

    sc.stop()
  }

  // Este método pinta el contenido de cada partición de un RDD.
  def myfunc(index: Int, iter: Iterator[(Any, Any)]) : Iterator[Any] = {
    iter.toList.map(x => "[partID: " +  index + ", val: " + x + "]").iterator
  }

  def firstExample(sc: SparkContext): Unit = {
    val a = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
    val b = sc.parallelize(List(1,1,2,2,2,1,2,2,2), 3)
  }
}
