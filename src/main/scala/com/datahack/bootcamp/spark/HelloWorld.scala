package com.datahack.bootcamp.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld extends App {

  val sparkAppConf: SparkConf = new SparkConf()
    .setAppName("Hello World")
    .setMaster("local[2]")

  val sparkContext: SparkContext = new SparkContext(sparkAppConf)

  val fileUrl: String = "src/main/resources/words.txt"

  val file: RDD[String] = sparkContext.textFile(fileUrl)

  val totalWords: RDD[String] = file.flatMap(line => line.split(" "))

  println(s"-- Total words: ${totalWords.count()}")

  sparkContext.stop()

}
