package com.datahack.bootcamp.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld extends App {

  val sparkConf = new SparkConf()
    .setAppName("Hello world")
    .setMaster("local[2]")

  val sparkContext = new SparkContext(sparkConf)

  val fileUrl = "src/main/resources/words.txt"

  val file: RDD[String] = sparkContext.textFile(fileUrl)

  println(s"Num lines: ${file.count()}")

  val words = file.flatMap( line => line.split(" "))

  println(s"Num words: ${words.count()}")

  sparkContext.stop()
}
