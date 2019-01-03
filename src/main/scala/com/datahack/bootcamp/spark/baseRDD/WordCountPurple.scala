package com.datahack.bootcamp.spark.baseRDD

import org.apache.spark.{SparkConf, SparkContext}

object WordCountPurple {

  //  Hacer de dos maneras distintas usando únicamente map, flatMap o filter el siguiente ejercicio:
  //  Dar la palabra y el número de letras de las palabras que contengan la letra l del fichero purple.txt

  lazy val purpleTextUrl = "src/main/resources/purple.txt"

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("Word Count Purple")
      .setMaster("local[2]")
    val sc = new SparkContext(conf)

    firstExample(sc)
    secondExample(sc)
    sc.stop()
  }

  def firstExample(sc: SparkContext): Unit = {
    val file = sc.textFile(purpleTextUrl)
    val words = file.flatMap(_.split(" ")).filter(w => w.contains("l")).map(w => (w,w.length)).collect()
    println("------- Result ---------")
    words.foreach(println)
  }

  def secondExample(sc: SparkContext): Unit = {
    val file = sc.textFile(purpleTextUrl)
    val words = file.flatMap(_.split(" ")).flatMap(word => if(word.contains("l")) Seq((word, word.length)) else Seq()).collect()
    println("------- Result ---------")
    words.foreach(println)
  }
}
