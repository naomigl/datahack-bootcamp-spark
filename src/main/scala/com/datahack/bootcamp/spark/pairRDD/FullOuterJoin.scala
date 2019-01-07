package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable

object FullOuterJoin {

  val fileAurl = "src/main/resources/join1_FileA.txt"
  val fileBurl = "src/main/resources/join1_FileB.txt"

  def main(args: Array[String]) {
    val conf: SparkConf = ???
    val sc: SparkContext = ???

    sc.stop()
  }

  def split_fileA(line: String): (String, Int) = ???

  def split_fileB(line: String): (String, String) = ???
}
