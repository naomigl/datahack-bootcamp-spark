package com.datahack.bootcamp.spark.broadcastaccumulator

import org.apache.spark.{SparkConf, SparkContext}

object Broadcast {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setAppName("Accumulator")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val items: List[(String, Double)] = List[(String, Double)] (
      ("item1", 12.8),
      ("item2", 3.4),
      ("item3", 10.0),
      ("item4", 23.3))

    val tax = sc.broadcast[Double](0.21)

    val res = sc.parallelize(items).mapValues(i => (i, i * tax.value)).collect()
    println("-------------Items:")
    res.foreach(i => println(s"${i._1} [Price = ${i._2._1} Taxes = ${i._2._2}]" ))
    sc.stop()
  }

}
