package com.datahack.bootcamp.spark.exercises.twitter.model

import play.api.libs.json.Json
import play.api.libs.json.Reads
import play.api.libs.json.Writes

case class EnrichedTweet(
                          id: Long,
                          author: Author,
                          text: String,
                          date: Long,
                          mentions: List[String],
                          hashTags: List[String],
                          urls: List[String],
                          isRT: Boolean,
                          favoriteCount: Long,
                          RTCount: Long,
                          sentiment: Option[String],
                          entities: List[String]
                        ) extends Serializable

object EnrichedTweet {
  implicit val writer: Writes[EnrichedTweet] = Json.writes[EnrichedTweet]
  implicit val reader: Reads[EnrichedTweet] = Json.reads[EnrichedTweet]
}
