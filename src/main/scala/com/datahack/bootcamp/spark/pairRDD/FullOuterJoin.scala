package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

object FullOuterJoin {

  val fileAurl = "src/main/resources/join1_FileA.txt"
  val fileBurl = "src/main/resources/join1_FileB.txt"

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val fileA: RDD[String] = sc.textFile(fileAurl)
    val fileB: RDD[String] = sc.textFile(fileBurl)

    val fileA_data: RDD[(String, Int)] = fileA.map(split_fileA)
    val fileB_data: RDD[(String, String)] = fileB.map(split_fileB)

    val fileB_joined_fileA: RDD[(String, (Option[String], Option[Int]))] = fileB_data.fullOuterJoin(fileA_data)
    val result: immutable.Seq[(String, (Option[String], Option[Int]))] = fileB_joined_fileA.collect().toList

    println(result)

    sc.stop()
  }

  def split_fileA(line: String): (String, Int) = {
    // Dividimos la línea del fichero en palabra u total separadaos por una coma
    val splitedLine: Array[String] = line.split(",")
    val word: String = splitedLine(0)
    // Convertimos el toal en entero
    val count: Int = splitedLine(1).toInt
    // Devolvemos una tupla para crear el Pair RDD
    (word, count)
  }

  def split_fileB(line: String): (String, String) = {
    // Dividimos la línea del fichero en palabra, fecha y total
    val splitedLineByComma: Array[String] = line.split(",")
    val count_string: String = splitedLineByComma(1)
    val wordAndDate: String = splitedLineByComma(0)
    val splitedLineByBlank: Array[String] = wordAndDate.split(" ")
    val date: String = splitedLineByBlank(0)
    val word: String = splitedLineByBlank(1)
    (word, date + " " + count_string)
  }
}
