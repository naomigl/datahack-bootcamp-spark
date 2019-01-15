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
  conf.setOAuthConsumerKey("fOo0Na4giQ992KmYFcgQspKKg")
  conf.setOAuthConsumerSecret("ftW5CcCIb7mIVmLzGBRCeeC1dwVR3LwWJp7klsmVVsrn2mlGug")
  conf.setOAuthAccessToken("39812074-u1PB7wSt2yz7cTjY2V4hBz7EWobHVlytXJYPFzuyC")
  conf.setOAuthAccessTokenSecret("dvGMVZaGaqV3bY7jUJDYiYdapgSUmJn3cDlW2mrtgnTRQ")

  val rootLogger = Logger.getRootLogger
  rootLogger.setLevel(Level.ERROR)

  val sparkConf: SparkConf = new SparkConf()
    .setAppName("Tutorial")
    .setMaster("local[2]")
  val ssc = new StreamingContext(sparkConf, Seconds(5))
  ssc.checkpoint("target/tmp")

  // Creamos el stream de entrada desde Twitter
  val auth: Authorization = AuthorizationFactory.getInstance(conf.build())
  val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, twitterAuth = Some(auth))

  // Imprimimos el numero de tweets por batch
  //stream.filter(status => status.getLang == "en").count().print()

  // Imprimirmos cada 10 senundos el número de tweets recibidos en los último 30 segunds
  stream.filter(status => status.getLang == "en").countByWindow(Seconds(30), Seconds(10)).print()
  println(s"Start Time: ${System.currentTimeMillis()}")
  ssc.start()
  ssc.awaitTermination()
}