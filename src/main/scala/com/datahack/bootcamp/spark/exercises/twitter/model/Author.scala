package com.datahack.bootcamp.spark.exercises.twitter.model

import play.api.libs.json.Json
import play.api.libs.json.Reads
import play.api.libs.json.Writes

case class Author(
                   id: Long,
                   name: String,
                   screenName: Option[String],
                   locationName: Option[String],
                   location: Option[Location]
                 )

object Author {
  implicit val writer: Writes[Author] = Json.writes[Author]
  implicit val reader: Reads[Author] = Json.reads[Author]
}

case class Location(lat: Double, lon: Double)

object Location {
  implicit val writer: Writes[Location] = Json.writes[Location]
  implicit val reader: Reads[Location] = Json.reads[Location]
}
