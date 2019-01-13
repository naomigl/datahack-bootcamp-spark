package com.datahack.bootcamp.spark.exercises.twitter

import com.datahack.bootcamp.spark.exercises.twitter.conf.{AppProperties, TwitterConfig}
import com.datahack.bootcamp.spark.exercises.twitter.spark.SparkContextBuilder
import com.datahack.bootcamp.spark.exercises.twitter.utils.FileUtils
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import twitter4j.Status

object Main {

  def main(args: Array[String]): Unit = {
    lazy val conf = AppProperties.config
    val (sparkSession, ssc) = SparkContextBuilder.createSessionStreamingContexts(conf)
    implicit val session = sparkSession
    implicit val sqlContext = session.sqlContext
    ssc.checkpoint("/tmp/")

    val rootLogger = Logger.getRootLogger
    rootLogger.setLevel(Level.ERROR)

    val entitiesList = FileUtils.parseEntitiesFile(AppProperties.getEntitiesFileUrl)
    implicit val bEntities: Broadcast[List[String]] = sparkSession.sparkContext.broadcast(entitiesList)

    import com.datahack.bootcamp.spark.exercises.twitter.spark.TweetAnalysisDsl._

    val stream = TwitterUtils.createStream(ssc, twitterAuth = TwitterConfig.getAuthorizationFactoryInstance)
    val filteredTweets: DStream[Status] = stream.filter(_.getLang == "es").cache()

    val statusAnalizedStream = filteredTweets.extractEntitiesAndMentions.cache()
    statusAnalizedStream.analyzeTweet.enrichTweet.persistElasticsearch

    ssc.start()
    ssc.awaitTermination()

  }
}
