package com.datahack.bootcamp.spark.exercises.twitter.conf

import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory

import scala.util.Try

object AppProperties {

  lazy val config: Config = ConfigFactory.load()

  private val twitterDebugKey = "twitter.debug"
  private val twitterDebugDefaultValue = false
  private val twitterOauthConsumerKey = "twitter.oauth.consumerKey"
  private val twitterOauthConsumerSecretKey = "twitter.oauth.consumerSecret"
  private val twitterOauthAccessTokenKey = "twitter.oauth.accessToken"
  private val twitterOauthAccessTokenSecretKey = "twitter.oauth.accessTokenSecret"
  private val dandelionTokenKey = "dandelion.token"
  private val elasticsearchDatasourceKey = "elasticsearch.datasource"
  private val elasticsearchResourceKey = "elasticsearch.org.elasticsearch.spark.sql.resource"
  private val entitiesFileUrlKey = "entities.file.url"

  def getTwitterDebug: Boolean = Try(config.getBoolean(twitterDebugKey)).getOrElse(twitterDebugDefaultValue)
  def getTwitterOauthConsumerKey: String = Try(config.getString(twitterOauthConsumerKey)).getOrElse("")
  def getTwitterOauthConsumerSecret: String = Try(config.getString(twitterOauthConsumerSecretKey)).getOrElse("")
  def getTwitterOauthAccessToken: String = Try(config.getString(twitterOauthAccessTokenKey)).getOrElse("")
  def getTwitterOauthAccessTokenSecret: String = Try(config.getString(twitterOauthAccessTokenSecretKey)).getOrElse("")

  def getDandelionToken: String = Try(config.getString(dandelionTokenKey)).getOrElse("")

  def getDataSource: String = Try(config.getString(elasticsearchDatasourceKey)).getOrElse("")
  def getResource: String = Try(config.getString(elasticsearchResourceKey)).getOrElse("")

  def getElasticserachPorpertiesAsMap: Map[String, String] =
    Map(
      "org.elasticsearch.spark.sql.nodes" -> config.getString("elasticsearch.org.elasticsearch.spark.sql.nodes"),
      "org.elasticsearch.spark.sql.port" -> config.getString("elasticsearch.org.elasticsearch.spark.sql.port"),
      "resource" -> config.getString("elasticsearch.org.elasticsearch.spark.sql.resource")
    )

  def getConfAsMap(path: String): Map[String, String] = {
    import scala.collection.JavaConverters._
    val pathConfig = config.getConfig(path)
    pathConfig.entrySet().asScala
      .map(entry => entry.getKey -> pathConfig.getString(entry.getKey)).toMap
  }

  def getEntitiesFileUrl: String = Try(config.getString(entitiesFileUrlKey))
    .getOrElse("src/main/resources/entities.txt")
}
