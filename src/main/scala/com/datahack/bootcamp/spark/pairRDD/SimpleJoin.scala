package com.datahack.bootcamp.spark.pairRDD

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SimpleJoin {

  val channelFilesPathUrl = "src/main/resources/join2_genchanA.txt"
  val countFilesPathUrl = "src/main/resources/join2_gennumA.txt"

  def main(args: Array[String]) {
    val conf: SparkConf = new SparkConf()
      .setAppName("Simple Join 2")
      .setMaster("local[2]")
    val sc: SparkContext = new SparkContext(conf)

    val channels: RDD[String] = sc.textFile(channelFilesPathUrl)
    val views: RDD[String] = sc.textFile(countFilesPathUrl)

    val show_views: RDD[(String, Int)] = views.map(splitShowViews)
    val show_channel: RDD[(String, String)] = channels.map(splitShowChannel)

    //(Show, (Channel, Views))
    val joinedDataSet: RDD[(String, (String, Int))] = show_channel.join(show_views)
    val channelViews: RDD[(String, Int)] = joinedDataSet.map(extractChannelViews)

    val result: Array[(String, Int)] = channelViews.reduceByKey(_+_).collect()
    result.foreach(println)

    sc.stop()
  }

  def splitShowViews(line: String): (String, Int) = {
    val showViews: Array[String] = line.split(",")
    val show: String = showViews(0)
    val views: Int = showViews(1).replace(" ", "").toInt
    (show, views)
  }

  def splitShowChannel(line: String): (String, String) = {
    val showChannel: Array[String] = line.split(",")
    val show: String = showChannel(0)
    val channel: String = showChannel(1)
    (show, channel)
  }

  def extractChannelViews(show_views_channel: (String, (String, Int))): (String, Int) = show_views_channel._2
}
