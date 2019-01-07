package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SimpleJoin {

  val channelFilesPathUrl = "src/main/resources/join2_genchanA.txt"
  val countFilesPathUrl = "src/main/resources/join2_gennumA.txt"

  def main(args: Array[String]) {
    val conf: SparkConf = ???
    val sc: SparkContext = ???
  }

  def splitShowViews(line: String): (String, Int) = ???

  def splitShowChannel(line: String): (String, String) = ???

  def extractChannelViews(show_views_channel: (String, (String, Int))): (String, Int) = ???
}
