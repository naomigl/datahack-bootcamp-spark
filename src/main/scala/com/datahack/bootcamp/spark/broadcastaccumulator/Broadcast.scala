package com.datahack.bootcamp.spark.broadcastaccumulator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkConf, SparkContext}

object Broadcast {

  def main(args: Array[String]) {
    val conf: SparkConf = ???
    val sc: SparkContext = ???

    val items: List[(String, Double)] = List[(String, Double)] (
      ("item1", 12.8),
      ("item2", 3.4),
      ("item3", 10.0),
      ("item4", 23.3))

    val tax: Broadcast[Double] = ???

    val res: Array[(String, (Double, Double))] = ???
    println("-------------Items:")
    res.foreach(i => ???)
    sc.stop()
  }

}
