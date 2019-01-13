package com.datahack.bootcamp.spark.exercises.twitter.spark

import com.datahack.bootcamp.spark.exercises.twitter.conf.AppProperties
import com.datahack.bootcamp.spark.exercises.twitter.model.{AnalysisResult, Author, EnrichedTweet, Location}
import com.datahack.bootcamp.spark.exercises.twitter.sentimentanalysis.StandfordNlpProvider
import com.datahack.bootcamp.spark.exercises.twitter.sentimentanalysis.dandelion.DandelionProvider
import com.datahack.bootcamp.spark.exercises.twitter.utils.{GoogleMapsUtils, TwitterUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
//import org.neo4j.spark.Neo4j
import twitter4j.Status

import scala.util.Try

class StatusAnalysisFunctions(self: DStream[Status])
                             (implicit session: SparkSession) {

  def extractEntitiesAndMentions: DStream[AnalysisResult] = {
    self.map { status =>
      AnalysisResult(
        status = status,
        entities = DandelionProvider.extractEntities(status.getText)
          .toOption.map(_.annotations.map(_.label)).getOrElse(List.empty[String]),
        mentions = TwitterUtils.extractTweetEntities(status)._1.map(_.getScreenName)
      )
    }
  }

}

class TweetAnalysisFunctions(self: DStream[AnalysisResult])
                            (implicit session: SparkSession, bEntities: Broadcast[List[String]]) extends Serializable {

  def filterTweetsByEntities: DStream[AnalysisResult] = {
    val entities: List[String] = bEntities.value
    self.filter { result =>
      (result.entities ++ result.mentions).map(
        e => entities.contains(e.toLowerCase)
      ).foldLeft(false)(_ || _)
    }
  }

  def analyzeTweet: DStream[AnalysisResult] = {
    self.map( result =>
      result.copy(
        sentiment = StandfordNlpProvider.extractSentiment(result.status.getText).toOption)
    )
  }

  def enrichTweet: DStream[EnrichedTweet] = {
    self.filter(!_.status.isRetweet).map{ status =>
      val tweetEntities = TwitterUtils.extractTweetEntities(status.status)
      EnrichedTweet(
        id = status.status.getId,
        author = Author(
          id = status.status.getUser.getId,
          screenName = Try(status.status.getUser.getScreenName).toOption,
          name = status.status.getUser.getName,
          locationName = Try(status.status.getUser.getLocation.toString).toOption,
          location = Try(status.status.getUser.getLocation.toString).toOption
            .flatMap(l => GoogleMapsUtils.getLatLong(l.split(",").toList)
              .map(l => Location(l._1, l._2)))
        ),
        text = status.status.getText,
        date = status.status.getCreatedAt.getTime,
        mentions = tweetEntities._1.map(_.getScreenName),
        hashTags = tweetEntities._2.map(_.getText),
        urls = tweetEntities._3.map(_.getURL),
        isRT = status.status.isRetweet,
        favoriteCount = status.status.getFavoriteCount,
        RTCount = status.status.getRetweetCount,
        sentiment = status.sentiment,
        entities = status.entities
      )
    }
  }

  /*def crawlRTsAndPersist: Unit = {
    import com.rafaelgarrote.utad.twitterstreaming.spark.GenericWriter._
    import session.sqlContext.implicits._

    self.filter(_.status.isRetweet).map{ status =>
      val author = status.status.getRetweetedStatus.getUser
      val rtBy = status.status.getUser
      RTEdge(
        authorName = author.getName,
        authorScreenName = author.getScreenName,
        authorId = author.getId,
        tweetId = status.status.getRetweetedStatus.getId,
        rtByName = rtBy.getName,
        rtByScreenName = rtBy.getScreenName,
        rtById = rtBy.getId,
        sentiment = status.sentiment.get
      )
    }.foreachRDD(rdd => rdd.toDF.writeNeo4jDF)
  }*/

}

class EnrichedTweetFunctions(self: DStream[EnrichedTweet])
                            (implicit session: SparkSession) extends Serializable {

  lazy val datasource: String = AppProperties.getDataSource
  lazy val options: Map[String, String] = AppProperties.getElasticserachPorpertiesAsMap

  def persistElasticsearch: Unit = {
    import com.datahack.bootcamp.spark.exercises.twitter.utils.GenericWriter._
    import session.sqlContext.implicits._
    self.foreachRDD(rdd => rdd.toDF().writeDF(datasource, options))
  }

}

trait TweetAnalysisDsl {

  implicit def analysisFunctions(dStream: DStream[Status])
                                (implicit session: SparkSession) =
    new StatusAnalysisFunctions(dStream)

  implicit def tweetAnalysisFunctions(dStream: DStream[AnalysisResult])
                                     (implicit session: SparkSession, bEntities: Broadcast[List[String]]) =
    new TweetAnalysisFunctions(dStream)

  implicit def enrichedTweetsFunctions(dStream: DStream[EnrichedTweet])
                                      (implicit session: SparkSession) =
    new EnrichedTweetFunctions(dStream)

}

object TweetAnalysisDsl extends TweetAnalysisDsl