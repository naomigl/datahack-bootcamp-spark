package com.datahack.bootcamp.spark.exercises.twitter.spark

import com.datahack.bootcamp.spark.exercises.twitter.conf.AppProperties
import com.typesafe.config.Config
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext

object SparkContextBuilder {

  def createSessionStreamingContexts(config: Config): (SparkSession, StreamingContext) = {
    //    val seconds = config.getInt(ConfigConstants.SparkSeconds)
    val seconds = 10
    val sparkConf = getSparkConf
    val session = SparkSession.builder().config(sparkConf).getOrCreate()
    (session, new StreamingContext(session.sparkContext, Seconds(seconds)))
  }

  private def getSparkConf: SparkConf = {
    val conf = new SparkConf().setAppName("Tweet Analysis").setMaster("local[2]")
    AppProperties.getConfAsMap("neo4j").map(entry => conf.set(entry._1, entry._2))
    conf
  }
}
