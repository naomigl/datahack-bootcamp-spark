package com.datahack.bootcamp.spark.broadcastaccumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

object Accumulator {

  def main(args: Array[String]) {
    val conf: SparkConf = ???
    val sc: SparkContext = ???

    val logFile: RDD[String] = sc.textFile("src/main/resources/error_log.log")

    val blankLines: Accumulator[Int] = ???

    val words: Array[String] = ???

    println(s"----- Blank lines: ${blankLines.value}")
    sc.stop()
  }

}
