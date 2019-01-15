package com.datahack.bootcamp.spark.streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import twitter4j.Status
import twitter4j.auth.{Authorization, AuthorizationFactory}
import twitter4j.conf.ConfigurationBuilder

object TrendingTopic extends App {

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
  val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(10))
  ssc.checkpoint("tmp")

  // Creamos el stream de entrada desde Twitter
  val auth: Authorization = AuthorizationFactory.getInstance(conf.build())

  //countTweetsEach20Seconds(ssc)
  //countEnglishTweetsEach20Seconds(ssc)
  // countEnglishTweetsLastMinute(ssc)
  // countTweetsByLangLastMinute(ssc)
  // englishHashTagsWithMoreThanOneOccurrences(ssc)
  // trendingTopic(ssc)

  // Número de tweets disponibles cada 10 segundos
  def countTweetsEach20Seconds(ssc: StreamingContext): Unit = {
    val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, twitterAuth = Some(auth))
    stream.count().print()

    ssc.start()
    ssc.awaitTermination()
  }

  // Número de tweets en inglés (en) cada 10 segundos
  def countEnglishTweetsEach20Seconds(ssc: StreamingContext): Unit = {
    val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, twitterAuth = Some(auth))
    stream.filter(tweet => tweet.getLang.equals("en")).count().print()

    ssc.start()
    ssc.awaitTermination()
  }

  // Número de tweets en inglés en el último minuto cada 20 secundos
  def countEnglishTweetsLastMinute(ssc: StreamingContext): Unit = {
    val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, twitterAuth = Some(auth))
    stream.filter(tweet => tweet.getLang.equals("en")).countByWindow(Minutes(1), Seconds(20)).print()

    ssc.start()
    ssc.awaitTermination()
  }
  // Número de tweets por idioma en el último minuto
  def countTweetsByLangLastMinute(ssc: StreamingContext): Unit = {
    val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, twitterAuth = Some(auth))
    stream.map(tweet => (tweet.getLang, 1)).reduceByKeyAndWindow((x, y) => x + y, Minutes(1)).print()

    ssc.start()
    ssc.awaitTermination()
  }
  // Hashtag y su número de ocurrencias para los tweets en inglés con más de una ocurrencia en los últimos 60 segundos
  // Ayuda: utiliza el método transform
  def englishHashTagsWithMoreThanOneOccurrences(ssc: StreamingContext) {
    val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, twitterAuth = Some(auth))
    stream.filter(tweet => tweet.getLang.equals("en"))
      .flatMap(tweet => tweet.getHashtagEntities)
      .map(hashTag => (hashTag.getText, 1)).reduceByKeyAndWindow((x, y) => x + y, Minutes(1))
      .transform(rdd => rdd.filter(_._2 > 1)).print()

    ssc.start()
    ssc.awaitTermination()
  }

  // Los diez hashtags más populares del último minuto para los tweets en inglés
  // Ayuda: utiliza el método transform y forEachRDD
  def trendingTopic(ssc: StreamingContext) {
    val stream: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, twitterAuth = Some(auth))
    stream.filter(tweet => tweet.getLang.equals("en"))
      .flatMap(tweet => tweet.getHashtagEntities)
      .map(hashTag => (hashTag.getText, 1)).reduceByKeyAndWindow((x, y) => x + y, Minutes(1))
      .transform(rdd => rdd.sortBy(hashtagPair => hashtagPair._2, false))
      .foreachRDD(rdd => {
        val topList = rdd.take(10)
        println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
        topList.foreach { case (tag, count) => println("%s (%d tweets)".format(tag, count)) }
      })

    ssc.start()
    ssc.awaitTermination()
  }
}
