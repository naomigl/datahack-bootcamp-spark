package com.datahack.bootcamp.spark.broadcastaccumulator

import org.apache.spark.{SparkConf, SparkContext}

object Accumulator {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Accumulator")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val logFile = sc.textFile("src/main/resources/error_log.log")

    val blankLines = sc.accumulator[Int](0)

    val words = logFile.flatMap(line => {
      if(line == "") {blankLines += 1}
      line.split(" ")
    }).collect()

    println(s"----- Blank lines: ${blankLines.value}")
    sc.stop()
  }

}
