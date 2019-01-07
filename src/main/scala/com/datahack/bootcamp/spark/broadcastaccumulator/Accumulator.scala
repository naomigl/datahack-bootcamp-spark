package com.datahack.bootcamp.spark.broadcastaccumulator

import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

object Accumulator {

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setAppName("Accumulator")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val logFile: RDD[String] = sc.textFile("src/main/resources/error_log.log")

    val blankLines: Accumulator[Int] = sc.accumulator[Int](0)

    val words: Array[String] = logFile.flatMap(line => {
      if(line == "") blankLines += 1
      line.split(" ")
    }).collect()

    println(s"----- Blank lines: ${blankLines.value}")
    sc.stop()
  }

}
