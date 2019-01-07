package com.datahack.bootcamp.spark.baseRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCountPurple {

  //  Hacer de dos maneras distintas usando únicamente map, flatMap o filter el siguiente ejercicio:
  //  Dar la palabra y el número de letras de las palabras que contengan la letra l del fichero purple.txt

  lazy val purpleTextUrl = "src/main/resources/purple.txt"

  def main(args: Array[String]) {
    val conf: SparkConf = ???
    val sc: SparkContext = ???

    firstExample(sc)
    secondExample(sc)
    sc.stop()
  }

  def firstExample(sc: SparkContext): Unit = {
    val file: RDD[String] = ???
    val words: Array[(String, Int)] = ???
    println("------- First Example ---------")
    words.foreach(println)
  }

  def secondExample(sc: SparkContext): Unit = {
    val file: RDD[String] = sc.textFile(purpleTextUrl)
    val words: Array[(String, Int)] = ???
    println("------- Second example ---------")
    words.foreach(println)
  }
}
