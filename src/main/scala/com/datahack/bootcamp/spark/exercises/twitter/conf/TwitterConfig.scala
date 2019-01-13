package com.datahack.bootcamp.spark.exercises.twitter.conf

import twitter4j.Twitter
import twitter4j.TwitterFactory
import twitter4j.auth.Authorization
import twitter4j.auth.AuthorizationFactory
import twitter4j.conf.ConfigurationBuilder

import scala.util.Try

object TwitterConfig {

  def confBuilder: ConfigurationBuilder = {
    val conf = new ConfigurationBuilder()
    conf.setDebugEnabled(AppProperties.getTwitterDebug)
    conf.setOAuthConsumerKey(AppProperties.getTwitterOauthConsumerKey)
    conf.setOAuthConsumerSecret(AppProperties.getTwitterOauthConsumerSecret)
    conf.setOAuthAccessToken(AppProperties.getTwitterOauthAccessToken)
    conf.setOAuthAccessTokenSecret(AppProperties.getTwitterOauthAccessTokenSecret)
    conf
  }

  def getAuthorizationFactoryInstance: Option[Authorization] =
    Try(AuthorizationFactory.getInstance(confBuilder.build())).toOption

  def getFactoryInstance: Option[Twitter] =
    Try(new TwitterFactory(confBuilder.build()).getInstance()).toOption

}
