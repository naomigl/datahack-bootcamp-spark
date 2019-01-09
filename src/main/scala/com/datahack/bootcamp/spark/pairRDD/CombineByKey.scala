package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CombineByKey {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf().setAppName("reduce").setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

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
    val c: RDD[(Int, String)] = b.zip(a)

    val spartition = c.mapPartitionsWithIndex(myfunc)

    val d = c.combineByKey(
      List(_),
      (x:List[String], y:String) => y :: x,
      (x: List[String], y: List[String]) => x ::: y
    )
    val result = d.collect()

    println("----------Example 1--------")
    spartition.foreach(println)
    println("--result:")
    result.foreach(println)
  }
}
