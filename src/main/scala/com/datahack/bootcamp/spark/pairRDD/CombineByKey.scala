package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.{SparkConf, SparkContext}

object CombineByKey {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    firstExample(sc)

    sc.stop()
  }

  // lets have a look at what is in the partitions
  def myfunc(index: Int, iter: Iterator[(Any, Any)]) : Iterator[Any] = {
    iter.toList.map(x => "[partID: " +  index + ", val: " + x + "]").iterator
  }

  def firstExample(sc: SparkContext): Unit = {
    val a = sc.parallelize(List("dog","cat","gnu","salmon","rabbit","turkey","wolf","bear","bee"), 3)
    val b = sc.parallelize(List(1,1,2,2,2,1,2,2,2), 3)
    val c = b.zip(a)

    val spartitions = c.mapPartitionsWithIndex(myfunc).collect()

    val d = c.combineByKey(
      List(_),
      (x:List[String], y:String) => y :: x,
      (x:List[String], y:List[String]) => x ::: y)
    val result = d.collect

    println("---------- Example 1 ----------")
    spartitions.foreach(println)
    println("-- Result: ")
    result.foreach(println)
  }
}
