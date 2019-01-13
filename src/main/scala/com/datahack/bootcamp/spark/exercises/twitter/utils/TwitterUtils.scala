package com.datahack.bootcamp.spark.exercises.twitter.utils

import com.datahack.bootcamp.spark.exercises.twitter.conf.TwitterConfig
import twitter4j.HashtagEntity
import twitter4j.MediaEntity
import twitter4j.Status
import twitter4j.SymbolEntity
import twitter4j.Twitter
import twitter4j.URLEntity
import twitter4j.User
import twitter4j.UserMentionEntity

import scala.util.matching.Regex

object TwitterUtils {

  lazy private val twitterFactoryInstance: Option[Twitter] = TwitterConfig.getFactoryInstance
  lazy private val userRegex: Regex = """^.*RT @[a-zA-Z0-9_]{1,15}""".r

  //http://twitter4j.org/javadoc/twitter4j/Status.html
  def extractTweetEntities(status: Status):
  (List[UserMentionEntity], List[HashtagEntity], List[URLEntity], List[SymbolEntity], List[MediaEntity]) = {
    val mentions: List[UserMentionEntity] = status.getUserMentionEntities.toList
    val hashTags: List[HashtagEntity] = status.getHashtagEntities.toList
    val urls: List[URLEntity] = status.getURLEntities.toList
    val symbols: List[SymbolEntity] = status.getSymbolEntities.toList
    val media: List[MediaEntity] = status.getMediaEntities.toList
    (mentions, hashTags, urls, symbols, media)
  }

  def extractRetweedUserScreenName(status: Status): Option[String] = {
    userRegex.findFirstIn(status.getText).map(_.split("@").last)
  }

  def extractRetweedUserProfile(status: Status): Option[User] = {
    extractRetweedUserScreenName(status).flatMap(user => getUserProfile(user))
  }

  def getUserProfile(userId: Long): Option[User] = twitterFactoryInstance.map(_.showUser(userId))

  def getUserProfile(screenName: String): Option[User] = twitterFactoryInstance.map(_.showUser(screenName))

  def getAllUserFriends(userId: Long): List[Long] = {

    def getNetxFriends(twitter: Twitter, cursor: Long, previousFriends: List[Long]): List[Long] = {
      cursor match {
        case 0 => previousFriends
        case _ =>
          val response = twitter.getFriendsIDs(userId, cursor)
          getNetxFriends(twitter, response.getNextCursor, (response.getIDs ++ previousFriends).toList)
      }
    }

    twitterFactoryInstance.map(getNetxFriends(_, -1, List.empty[Long])).getOrElse(List.empty[Long])
  }

  def getAllUserFriends(userId: Long, limit: Int): List[Long] = {

    def getNextFriends(twitter: Twitter, cursor: Long, previousFriends: List[Long]): List[Long] = {
      cursor match {
        case 0 => previousFriends
        case _ =>
          val response = twitter.getFriendsIDs(userId, cursor, limit)
          getNextFriends(twitter, response.getNextCursor, (response.getIDs ++ previousFriends).toList)
      }
    }

    twitterFactoryInstance.map(getNextFriends(_, -1, List.empty[Long])).getOrElse(List.empty[Long])
  }

  def getAllUserFollowers(userId: Long): List[Long] = {

    def getNextFollowers(twitter: Twitter, cursor: Long, previousFriends: List[Long]): List[Long] = {
      cursor match {
        case 0 => previousFriends
        case _ =>
          val response = twitter.getFollowersIDs(userId, cursor)
          getNextFollowers(twitter, response.getNextCursor, (response.getIDs ++ previousFriends).toList)
      }
    }

    twitterFactoryInstance.map(getNextFollowers(_, -1, List.empty[Long])).getOrElse(List.empty[Long])
  }

  def getAllUserFollowers(userId: Long, limit: Int): List[Long] = {

    def getNextFollowers(twitter: Twitter, cursor: Long, previousFriends: List[Long]): List[Long] = {
      cursor match {
        case 0 => previousFriends
        case _ =>
          val response = twitter.getFollowersIDs(userId, cursor, limit)
          getNextFollowers(twitter, response.getNextCursor, (response.getIDs ++ previousFriends).toList)
      }
    }

    twitterFactoryInstance.map(getNextFollowers(_, -1, List.empty[Long])).getOrElse(List.empty[Long])
  }

}
