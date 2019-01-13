package com.datahack.bootcamp.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, streaming}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import twitter4j.Status
import twitter4j.auth.{Authorization, AuthorizationFactory}
import twitter4j.conf.ConfigurationBuilder

object TwitterExample extends App {

  // Configuramos las credenciales
  val conf = new ConfigurationBuilder()
  conf.setDebugEnabled(true)
  conf.setOAuthConsumerKey("")
  conf.setOAuthConsumerSecret("")
  conf.setOAuthAccessToken("")
  conf.setOAuthAccessTokenSecret("")

  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("Tutorial")
    .setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(5))
  ssc.checkpoint("tmp")

  // Creamos el stream de entrada desde Twitter
  val auth: Authorization = AuthorizationFactory.getInstance(conf.build())
  val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, twitterAuth = Some(auth))

  // Imprimimos el numero de tweets por batch
  //stream.map(status => status.getLang).count().print()

  // Imprimirmos cada 10 senundos el número de tweets recibidos en los último 30 segunds
  stream.map(status => status.getLang).countByWindow(Seconds(30), Seconds(10)).print()
  println(s"Start Time: ${System.currentTimeMillis()}")
  ssc.start()
  ssc.awaitTermination()
}